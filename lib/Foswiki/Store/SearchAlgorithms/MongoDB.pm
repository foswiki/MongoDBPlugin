# See bottom of file for license and copyright information

package Foswiki::Store::SearchAlgorithms::MongoDB;

use strict;
use Assert;
use Foswiki::Plugins::MongoDBPlugin;
use Foswiki::Plugins::MongoDBPlugin::DB;
use Foswiki::Search::MongoDBInfoCache;

#use Data::Dumper;

use Foswiki::Store::Interfaces::QueryAlgorithm ();
our @ISA = ('Foswiki::Store::Interfaces::QueryAlgorithm');

use constant MONITOR => 0;

BEGIN {

#enable the MongoDBPlugin which keeps the mongodb uptodate with topics changes onsave
#TODO: make conditional - or figure out how to force this in the MongoDB search and query algo's
    $Foswiki::cfg{Plugins}{MongoDBPlugin}{Module} =
      'Foswiki::Plugins::MongoDBPlugin';
    $Foswiki::cfg{Plugins}{MongoDBPlugin}{Enabled}             = 1;
    $Foswiki::cfg{Plugins}{MongoDBPlugin}{EnableOnSaveUpdates} = 1;
    print STDERR "****** starting MongoDBPlugin..\n" if MONITOR;
    
    $Foswiki::Plugins::SESSION->{store}->setListenerPriority('Foswiki::Plugins::MongoDBPlugin::Listener', 1);

}

=begin TML

---++ ClassMethod new( $class,  ) -> $cereal

=cut

sub new {
    my $self = shift()->SUPER::new( 'SEARCH', @_ );
    return $self;
}

=begin TML

---+ package Foswiki::Store::SearchAlgorithms::MongoDB

A very simplistic conversion from PurePerl to querying mongoDB

still doing a very dumb iteration through the queries and then aggregating it in perl

also, its currently ignoring the input result set.

---++ search($searchString, $inputTopicSet, $session, $options) -> \%seen
Search .txt files in $dir for $string. See RcsFile::searchInWebContent
for details.

DEPRECATED


=cut

sub search {
    my ( $searchString, $web, $inputTopicSet, $session, $options ) = @_;

    local $/ = "\n";
    my %seen;
    if ( $options->{type} && $options->{type} eq 'regex' ) {

        # Escape /, used as delimiter. This also blocks any attempt to use
        # the search string to execute programs on the server.
        $searchString =~ s!/!\\/!g;
    }
    else {

        # Escape non-word chars in search string for plain text search
        $searchString =~ s/(\W)/\\$1/g;
    }

    # Convert GNU grep \< \> syntax to \b
    $searchString =~ s/(?<!\\)\\[<>]/\\b/g;
    $searchString =~ s/^(.*)$/\\b$1\\b/go if $options->{'wordboundaries'};

    #$searchString =~ s/\\"/./g;
    #$searchString =~ s/\\b//g;

    my $casesensitive =
      defined( $options->{casesensitive} ) ? $options->{casesensitive} : 1;

    my %elements;

    push( @{ $elements{_web} }, $web );

    #dont' add a search for all..
    if ( $searchString ne '.*' ) {
        push(
            @{ $elements{_raw_text} },
            {
                '$regex'   => $searchString,
                '$options' => ( $casesensitive ? '' : 'i' )
            }
        );
    }

    my $cursor = Foswiki::Plugins::MongoDBPlugin::getMongoDB()
      ->query( $web, 'current', \%elements );
    return new Foswiki::Search::MongoDBInfoCache( $Foswiki::Plugins::SESSION,
        $web, $options, $cursor );
}

#ok, for initial validation, naively call the code with a web.
sub _webQuery {
    my ( $this, $query, $web, $inputTopicSet, $session, $options ) = @_;
    ASSERT( scalar( @{ $query->{tokens} } ) > 0 ) if DEBUG;

    # default scope is 'text'
    $options->{'scope'} = 'text'
      unless ( defined( $options->{'scope'} )
        && $options->{'scope'} =~ /^(topic|all)$/ );

    my $topicSet = $inputTopicSet;

    #print STDERR "######## Search::MongoDB query ($web) tokens "
    #  . scalar( @{ $query->{tokens} } ) . " : "
    #  . join( ',', @{ $query->{tokens} } ) . "\n";

#TODO:
#               the query & search functions in the query&search algo just _create_ the hash for the query
#               and this is stored in the topic Set. When the topic set is 'evaluated' the query is sent (by the topic set)
#               and from there the cursor is used.
#nonetheless, the rendering of 2000 results takes much longer than the querying, but as the 2 are on separate servers, everything is golden :)

#use IxHash to keep the hash order - _some_ parts of queries are order sensitive
    my %mongoQuery = ();
    my $ixhQuery = tie( %mongoQuery, 'Tie::IxHash' );

    #    $ixhQuery->Push( $scope => $elem );

#TODO: Mongo advanced query docco indicates that /^a/ is faster than /^a.*/ and /^a.*$/ so should refactor to that.
    my $includeTopicsRegex =
      Foswiki::Search::MongoDBInfoCache::convertTopicPatternToRegex(
        $options->{topic} );

#print STDERR "-------------------- _topic => $includeTopicsRegex (".$options->{topic}.")\n";
    if (    ( $includeTopicsRegex ne '' )
        and ( $includeTopicsRegex ne '.*' ) )
    {
        $includeTopicsRegex = qr/$includeTopicsRegex/;
        $ixhQuery->Push( '_topic' => $includeTopicsRegex );
    }

    my $excludeTopicsRegex =
      Foswiki::Search::MongoDBInfoCache::convertTopicPatternToRegex(
        $options->{excludetopic} );

    #BUGGGGGG - can't add topic= and excludetopic= - same key, it go boom
    #WORSE - there is no actual way to do A and not B in mongodb?
    #print STDERR "--------------------2 _topic => $includeTopicsRegex\n";
    if ( $excludeTopicsRegex ne '' ) {
        $excludeTopicsRegex = qr/$excludeTopicsRegex/;
        $ixhQuery->Push( '_topic' => { '$not' => $excludeTopicsRegex } );
    }

    $ixhQuery->Push( '_web' => $web );

    my $casesensitive =
      defined( $options->{casesensitive} ) ? $options->{casesensitive} : 0;

    my $counter             = 0;
    my $mongoJavascriptFunc = '';
    foreach my $raw_token ( @{ $query->{tokens} } ) {
        my $token = $raw_token;
        my $topic_searchString;
        my $raw_searchString;

        # flag for AND NOT search
        my $invertSearch = 0;
        $invertSearch = ( $token =~ s/^\!//o );

        #TODO: work out why mongo hates ^%META
        #TODO: make a few more unit tests with ^ in them
        #(adding 'm' to the options isn't it
        $token =~ s/\^%META/%META/g;

        # scope can be 'topic' (default), 'text' or "all"
        # scope='topic', e.g. Perl search on topic name:
        if ( $options->{'scope'} ne 'text' ) {
            $topic_searchString = $token;

            # FIXME I18N
            if ( $options->{'type'} ne 'regex' ) {
                $topic_searchString = quotemeta($topic_searchString);
            }
        }

# scope='text', e.g. grep search on topic text:
#TODO: this is actually incorrect for scope="both", as we need to OR the _topic and _topic_raw results SOOO, we fake it further up
        if ( $options->{'scope'} ne 'topic' ) {
            $raw_searchString = $token;
            if ( $options->{type} && $options->{type} eq 'regex' ) {

              # Escape /, used as delimiter. This also blocks any attempt to use
              # the search string to execute programs on the server.
                $raw_searchString =~ s!/!\/!g;
            }
            else {

                # Escape non-word chars in search string for plain text search
                $raw_searchString =~ s/(\W)/\\$1/g;
            }

            # Convert GNU grep \< \> syntax to \b
            $raw_searchString =~ s/(?<!\\)\\[<>]/\\b/g;
            $raw_searchString =~ s/^(.*)$/\\b$1\\b/go
              if $options->{'wordboundaries'};
        }

        #remove pointless regex..
        #TODO: need to work out wtf '\.*'
        $raw_searchString = undef
          if ( defined($raw_searchString) and $raw_searchString eq '.*' );
        $topic_searchString = undef
          if ( defined($topic_searchString) and $topic_searchString eq '.*' );

        if ( $counter == 0 ) {
            my $raw_text_regex =
              convertQueryToMongoRegex( $raw_searchString, $casesensitive,
                $invertSearch );
            my $topic_regex =
              convertQueryToMongoRegex( $topic_searchString, $casesensitive,
                $invertSearch );

            if (    ( defined($topic_regex) and defined($raw_text_regex) )
                and ( not $invertSearch ) )
            {
                $ixhQuery->Push(
                    '$or' => [
                        { '_topic'  => $topic_regex },
                        { _raw_text => $raw_text_regex }
                    ]
                );
            }
            else {
                $ixhQuery->Push( '_topic' => $topic_regex )
                  if defined($topic_regex);
                $ixhQuery->Push( '_raw_text' => $raw_text_regex )
                  if defined($raw_text_regex);
            }
        }
        else {

    #need to write the additional tokens as js. can't do _topic: {/qwe/, /asdf/}
            if ( ( defined($topic_searchString) and defined($raw_searchString) )
                and ( not $invertSearch ) )
            {

#$ixhQuery->Push( '$or' => [{'_topic' => $topic_regex}, {_raw_text => $raw_text_regex}] );
#need to OR and NOT the regexStrings.......
                $mongoJavascriptFunc .= convertQueryToJavascript(
                    'query' . $counter,
                    'HASH',
                    {
                        '_raw_text' => $raw_searchString,
                        '_topic'    => $topic_searchString
                    },
                    ( $casesensitive ? '' : 'i' ),
                    $invertSearch
                );
            }
            else {
                if ( defined($raw_searchString) ) {
                    $mongoJavascriptFunc .= convertQueryToJavascript(
                        'query' . $counter,
                        '_raw_text', $raw_searchString,
                        ( $casesensitive ? '' : 'i' ),
                        $invertSearch
                    );
                }
                if ( defined($topic_searchString) ) {
                    $mongoJavascriptFunc .= convertQueryToJavascript(
                        'query' . $counter,
                        '_topic', $topic_searchString,
                        ( $casesensitive ? '' : 'i' ),
                        $invertSearch
                    );
                }
            }
        }
        $counter++;
    }    #end foreach
    if ( $mongoJavascriptFunc ne '' ) {
        $mongoJavascriptFunc =
          'function() {' . $mongoJavascriptFunc . 'return true;}';
        $ixhQuery->Push( '$where' => $mongoJavascriptFunc );
        print STDERR "------$mongoJavascriptFunc\n" if MONITOR;
    }

    #add ACL filter
    my $userIsIn = Foswiki::Plugins::MongoDBPlugin::getACLProfilesFor($session->{user}, $web);
    ### ((_ACLProfile_ALLOWTOPICVIEW: $in(userIsIn, UNDEF)) AND (_ACLProfile.DENYTOPICVIEW: $NOTin(userIsIn)))
    #TODO: this is incorrect, it needs to also have the logic for the web default (and be inverted if the web DENYs the user..
    if ($session->access->haveAccess('VIEW', $session->{user}, $web)) {
        #TODO: potential BUG - if user is in both allow and deny, the algo chooses allow
        #$mongoQuery->{_ACLProfile_ALLOWTOPICVIEW} = {'$in' => [@$userIsIn, 'UNDEFINED']};
        #$mongoQuery->{_ACLProfile_DENYTOPICVIEW} = {'$nin' => $userIsIn};
        $ixhQuery->Push( '_ACLProfile_ALLOWTOPICVIEW' => {'$in' => [@$userIsIn, 'UNDEFINED']} );
        $ixhQuery->Push( '{_ACLProfile_DENYTOPICVIEW}' => {'$nin' => $userIsIn} );
    } else {
        #user is already denied, so we only get view access _if_ the user is specifically ALLOWed
        #$mongoQuery->{_ACLProfile_ALLOWTOPICVIEW} = {'$in' => [@$userIsIn]};
        $ixhQuery->Push( '_ACLProfile_ALLOWTOPICVIEW' => {'$in' => $userIsIn} );
    }

    #limit, skip, sort_by
    my $SortDirection = Foswiki::isTrue( $options->{reverse} ) ? -1 : 1;

    #ME bets casesensitive Sorting has no unit tests..
    #order="topic"
    #order="created"
    #order="modified"
    #order="editby"
    #order="formfield(name)"
    #reverse="on"
    my %sortKeys = (
        topic => '_topic',

        #created => ,   #TODO: don't yet have topic histories in mongo
        modified => 'TOPICINFO.date',

#TODO: foswiki's sort by TOPICINFO.author sorts by WikiName, not CUID - so need to make an internal version of this
        editby => 'TOPICINFO._authorWikiName',
    );

    my $queryAttrs = {};
    my $orderBy = $sortKeys{ $options->{order} || 'topic' };
    if ( defined($orderBy) ) {
        $queryAttrs = { sort_by => { $orderBy => $SortDirection } };
    }
    else {
        if ( $options->{order} =~ /formfield\((.*)\)/ ) {
            $orderBy = 'FIELD.' . $1;
            $queryAttrs = { sort_by => { $orderBy => $SortDirection } };
        }
    }

    my $cursor = Foswiki::Plugins::MongoDBPlugin::getMongoDB()
      ->query( $web, 'current', $ixhQuery, $queryAttrs );

    return new Foswiki::Search::MongoDBInfoCache( $Foswiki::Plugins::SESSION,
        $web, $options, $cursor );
}

sub convertQueryToMongoRegex {
    my ( $searchString, $casesensitive, $invertSearch ) = @_;
    my $mongoRegexHash;

    if ( defined($searchString) ) {
        my $theRe = ( $casesensitive ? qr/$searchString/ : qr/$searchString/i );

        if ($invertSearch) {
            $mongoRegexHash = { '$not' => $theRe };
        }
        else {
            $mongoRegexHash = $theRe;
        }
    }
    return $mongoRegexHash;
}

sub convertQueryToJavascript {
    my $name         = shift;
    my $scope        = shift;
    my $regex        = shift;
    my $regexoptions = shift || '';
    my $not          = shift || '';

    if ( ( $scope eq 'HASH' ) and ( ref($regex) eq 'HASH' ) ) {
        my $not = $not ? '!' : '';

        #return (A OR B OR C)
        my $js    = "\t\t{\n\t\t\tvar ret = false;\n";
        my $count = 1;
        foreach my $scope ( keys(%$regex) ) {
            my $regex = $regex->{$scope};
            $js .= <<"HERE";
                { 
                    $name = /$regex/$regexoptions ; 
                    matched = $name.test(this.$scope);
                    ret = (ret || $not(matched));
                 }
HERE
            $count++;
        }
        $js .= "\t\t\tif (!ret) {return false;}\t\t}\n";
        return $js;
    }
    else {
        return '' if ( $regex eq '' );

        my $invertedNot = $not ? '' : '!';
        return <<"HERE";
                { 
                    $name = /$regex/$regexoptions ; 
                    matched = $name.test(this.$scope);
                    if ($invertedNot(matched)) {
                        return false; 
                    }
                 }
HERE
    }
    die "ERROR: unexpected regex param type" . ref($regex) . "\n";
}

1;
__DATA__
# Plugin for Foswiki - The Free and Open Source Wiki, http://foswiki.org/
#
# Copyright 2010-2011 - SvenDowideit@fosiki.com
#
# MongoDBPlugin is # This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# For licensing info read LICENSE file in the root of this distribution.
