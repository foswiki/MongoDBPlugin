# See bottom of file for license and copyright information

package Foswiki::Store::SearchAlgorithms::MongoDB;

use strict;
use Assert;
use Foswiki::Search::MongoDBInfoCache;

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

    my $casesensitive =
      defined( $options->{casesensitive} ) ? $options->{casesensitive} : 1;

    my %elements;

    push( @{ $elements{_web} }, $web );
    push(
                    @{ $elements{_raw_text} },
                    {
                        '$regex'   => $searchString,
                        '$options' => ( $casesensitive ? '' : 'i' )
                    }
                    );

    my $cursor = doMongoSearch( $web, $options, \%elements );
    return new Foswiki::Search::MongoDBInfoCache( $Foswiki::Plugins::SESSION,
        $web, $options, $cursor );
}

=begin TML

this is the new way -

=cut

sub query {
    my ( $query, $inputTopicSet, $session, $options ) = @_;

    if ( ( @{ $query->{tokens} } ) == 0 ) {
        return new Foswiki::Search::InfoCache( $session, '' );
    }

    my $webNames = $options->{web}       || '';
    my $recurse  = $options->{'recurse'} || '';
    my $isAdmin  = $session->{users}->isAdmin( $session->{user} );

    my $searchAllFlag = ( $webNames =~ /(^|[\,\s])(all|on)([\,\s]|$)/i );
    my @webs = Foswiki::Search::InfoCache::_getListOfWebs( $webNames, $recurse,
        $searchAllFlag );

    my @resultCacheList;
    foreach my $web (@webs) {

        # can't process what ain't thar
        next unless $session->webExists($web);

        my $webObject = Foswiki::Meta->new( $session, $web );
        my $thisWebNoSearchAll =
          Foswiki::isTrue( $webObject->getPreference('NOSEARCHALL') );

        # make sure we can report this web on an 'all' search
        # DON'T filter out unless it's part of an 'all' search.
        next
          if ( $searchAllFlag
            && !$isAdmin
            && ( $thisWebNoSearchAll || $web =~ /^[\.\_]/ )
            && $web ne $session->{webName} );

        my $infoCache =
          _webQuery( $query, $web, $inputTopicSet, $session, $options );
        $infoCache->sortResults($options);
        push( @resultCacheList, $infoCache );
    }
    my $resultset =
      new Foswiki::Search::ResultSet( \@resultCacheList, $options->{groupby},
        $options->{order}, Foswiki::isTrue( $options->{reverse} ) );

    #TODO: $options should become redundant
    $resultset->sortResults($options);
    return $resultset;
}

#ok, for initial validation, naively call the code with a web.
sub _webQuery {
    my ( $query, $web, $inputTopicSet, $session, $options ) = @_;
    ASSERT( scalar( @{ $query->{tokens} } ) > 0 ) if DEBUG;

    # default scope is 'text'
    $options->{'scope'} = 'text'
      unless ( defined( $options->{'scope'} )
        && $options->{'scope'} =~ /^(topic|all)$/ );

    my $topicSet = $inputTopicSet;

    print STDERR "######## Search::MongoDB query ($web) tokens "
      . scalar( @{ $query->{tokens} } ) . " : "
      . join( ',', @{ $query->{tokens} } ) . "\n";

#TODO:
#               the query & search functions in the query&search algo just _create_ the hash for the query
#               and this is stored in the topic Set. When the topic set is 'evaluated' the query is sent (by the topic set)
#               and from there the cursor is used.
#nonetheless, the rendering of 2000 results takes much longer than the querying, but as the 2 are on separate servers, everything is golden :)

    my %elements;

#TODO: Mongo advanced query docco indicates that /^a/ is faster than /^a.*/ and /^a.*$/ so should refactor to that.
    my $includeTopicsRegex =
      Foswiki::Search::MongoDBInfoCache::convertTopicPatternToRegex(
        $options->{topic} );
    my $excludeTopicsRegex =
      Foswiki::Search::MongoDBInfoCache::convertTopicPatternToRegex(
        $options->{excludetopic} );
    if ( $includeTopicsRegex ne '' ) {
        push( @{ $elements{_topic} }, { '$regex' => $includeTopicsRegex } );
    }
    if ( $excludeTopicsRegex ne '' ) {
        push(
            @{ $elements{_topic} },
            { '$not' => { '$regex' => $excludeTopicsRegex } }
        );
    }

    push( @{ $elements{_web} }, $web );

    my $casesensitive =
      defined( $options->{casesensitive} ) ? $options->{casesensitive} : 0;

    foreach my $token ( @{ $query->{tokens} } ) {

        # flag for AND NOT search
        my $invertSearch = 0;
        $invertSearch = ( $token =~ s/^\!//o );
        
        #TODO: work out why mongo hates ^%META
        #TODO: make a few more unit tests with ^ in them
        $token =~ s/^\^%META//;

#TODO: ** this code is totally broken when scope == all.
#probably need to reimplement this using the mapreduce querys with temporary collections that can be re-queried.

        # scope can be 'topic' (default), 'text' or "all"
        # scope='topic', e.g. Perl search on topic name:
        if ( $options->{'scope'} ne 'text' ) {
            my $searchString = $token;

            # FIXME I18N
            if ( $options->{'type'} ne 'regex' ) {
                $searchString = quotemeta($searchString);
            }

            if ($invertSearch) {
                push(
                    @{ $elements{_topic} },
                    {
                        '$not' => {
                            '$regex'   => $searchString,
                            '$options' => ( $casesensitive ? '' : 'i' )
                        }
                    }
                );
            }
            else {
                push(
                    @{ $elements{_topic} },
                    {
                        '$regex'   => $searchString,
                        '$options' => ( $casesensitive ? '' : 'i' )
                    }
                );
            }
        }

        # scope='text', e.g. grep search on topic text:
        if ( $options->{'scope'} ne 'topic' ) {
            my $searchString = $token;
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
            $searchString =~ s/^(.*)$/\\b$1\\b/go
              if $options->{'wordboundaries'};

            if ($invertSearch) {
                push(
                    @{ $elements{_raw_text} },
                    {
                        '$not' => {
                            '$regex'   => $searchString,
                            '$options' => ( $casesensitive ? '' : 'i' )
                        }
                    }
                );
            }
            else {
                push(
                    @{ $elements{_raw_text} },
                    {
                        '$regex'   => $searchString,
                        '$options' => ( $casesensitive ? '' : 'i' )
                    }
                );
            }
        }
    }    #end foreach

    my $cursor = doMongoSearch( $web, $options, \%elements );
    return new Foswiki::Search::MongoDBInfoCache( $Foswiki::Plugins::SESSION,
        $web, $options, $cursor );
}

sub doMongoSearch {
    my $web      = shift;
    my $options  = shift;
    my $elements = shift;

    print STDERR "######## Search::MongoDB search ($web)  \n";
    require Foswiki::Plugins::MongoDBPlugin;
    require Foswiki::Plugins::MongoDBPlugin::DB;
    my $collection =
      Foswiki::Plugins::MongoDBPlugin::getMongoDB()->_getCollection('current');

    my %mongoQuery = ();

#use IxHash to keep the hash order - leaving the javascript $where function to be called last.
    my $ixhQuery            = tie( %mongoQuery, 'Tie::IxHash' );
    my $mongoJavascriptFunc = '';
    my $counter             = 1;

    #pop off the first query element foreach scope and use that literally
    #foreach my $scope (keys(%{$elements})) {
    #lets order it so that we can reduce the test set quickly.
    foreach my $scope (qw/_topic _web _text _raw_text/) {
        foreach my $elem ( @{ $elements->{$scope} } ) {
            if ( !defined( $mongoQuery{$scope} ) ) {
                $ixhQuery->Push( $scope => $elem );
            }
            else {
                my $not = $elem->{'$not'};
                if ( defined($not) ) {
                    $elem = $not;
                    $not  = '!';
                }
                my $casesensitive = $elem->{'$options'};
                my $reg           = $elem->{'$regex'};
                $mongoJavascriptFunc .=
                  convertQueryToJavascript( 'query' . $counter,
                    $scope, $reg, $casesensitive, $not );
                $counter++;
            }
        }
    }
    if ( $counter > 1 ) {
        $mongoJavascriptFunc =
          'function() {' . $mongoJavascriptFunc . 'return true;}';
        $ixhQuery->Push( '$where' => $mongoJavascriptFunc );
        print STDERR "------$mongoJavascriptFunc\n";
    }

    my $cursor = $collection->query($ixhQuery);

    print STDERR "found " . $cursor->count . "\n";

    return $cursor;
}

sub convertQueryToJavascript {
    my $name         = shift;
    my $scope        = shift;
    my $regex        = shift;
    my $regexoptions = shift || '';
    my $not          = shift || '';
    my $invertedNot  = ( $not eq '!' ) ? '' : '!';

    return '' if ( $regex eq '' );

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

1;
__DATA__
# Plugin for Foswiki - The Free and Open Source Wiki, http://foswiki.org/
#
# Copyright 2010 - SvenDowideit@fosiki.com
#
# MongoDBPlugin is # This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# For licensing info read LICENSE file in the root of this distribution.
