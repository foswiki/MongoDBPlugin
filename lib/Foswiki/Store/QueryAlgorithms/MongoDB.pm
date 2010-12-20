# See the bottom of this file for license and copyright information

=begin TML

---+ package Foswiki::Store::QueryAlgorithms::MongoDB

Default brute-force query algorithm

Has some basic optimisation: it hoists regular expressions out of the
query to use with grep, so we can narrow down the set of topics that we
have to evaluate the query on.

Not sure exactly where the breakpoint is between the
costs of hoisting and the advantages of hoisting. Benchmarks suggest
that it's around 6 topics, though this may vary depending on disk
speed and memory size. It also depends on the complexity of the query.

=cut

package Foswiki::Store::QueryAlgorithms::MongoDB;
use strict;
use constant MONITOR => 1;

BEGIN {

#enable the MongoDBPlugin which keeps the mongodb uptodate with topics changes onsave
#TODO: make conditional - or figure out how to force this in the MongoDB search and query algo's
    $Foswiki::cfg{Plugins}{MongoDBPlugin}{Module} =
      'Foswiki::Plugins::MongoDBPlugin';
    $Foswiki::cfg{Plugins}{MongoDBPlugin}{Enabled}             = 1;
    $Foswiki::cfg{Plugins}{MongoDBPlugin}{EnableOnSaveUpdates} = 1;
    print STDERR "****** starting MongoDBPlugin..\n" if MONITOR;
}

use Foswiki::Search::Node ();
use Foswiki::Store::SearchAlgorithms::MongoDB();
use Foswiki::Plugins::MongoDBPlugin       ();
use Foswiki::Plugins::MongoDBPlugin::Meta ();
use Foswiki::Search::InfoCache;
use Foswiki::Plugins::MongoDBPlugin::HoistMongoDB;
use Data::Dumper;
use Assert;

# See Foswiki::Query::QueryAlgorithms.pm for details
sub query {
    my ( $query, $inputTopicSet, $session, $options ) = @_;

    # Fold constants
    my $context = Foswiki::Meta->new( $session, $session->{webName} );
    $query->simplify( tom => $context, data => $context );

    my $webNames = $options->{web}       || '';
    my $recurse  = $options->{'recurse'} || '';
    my $isAdmin  = $session->{users}->isAdmin( $session->{user} );

    my $searchAllFlag = ( $webNames =~ /(^|[\,\s])(all|on)([\,\s]|$)/i );
    my @webs =
      Foswiki::Plugins::MongoDBPlugin::_getListOfWebs( $webNames, $recurse,
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

        #TODO: combine these into one great ResultSet
        my $infoCache =
          _webQuery( $query, $web, $inputTopicSet, $session, $options );
        push( @resultCacheList, $infoCache );
    }
    my $resultset =
      new Foswiki::Search::ResultSet( \@resultCacheList, $options->{groupby},
        $options->{order}, Foswiki::isTrue( $options->{reverse} ) );

    #TODO: $options should become redundant
    $resultset->sortResults($options);
    return $resultset;
}

# Query over a single web
sub _webQuery {
    my ( $query, $web, $inputTopicSet, $session, $options ) = @_;

#SMELL: initialise the mongoDB hack. needed if the mondoPlugin is not enabled, but the algo is selected :/
    Foswiki::Plugins::MongoDBPlugin::getMongoDB();

    my $topicSet = $inputTopicSet;
    if ( !defined($topicSet) ) {

        #then we start with the whole web?
        #TODO: i'm sure that is a flawed assumption
        my $webObject = Foswiki::Meta->new( $session, $web );
        $topicSet =
          Foswiki::Search::InfoCache::getTopicListIterator( $webObject,
            $options );
    }
    if ( defined( $Foswiki::cfg{Plugins}{MongoDBPlugin}{ExperimentalCode} )
        and $Foswiki::cfg{Plugins}{MongoDBPlugin}{ExperimentalCode} )
    {

        #try HoistMongoDB first
        my $mongoQuery =
          Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);

        if ( defined($mongoQuery) ) {

            #TODO: where are we limiting the query to the $web?
            ASSERT( not defined( $mongoQuery->{'_web'} ) ) if DEBUG;
            $mongoQuery->{'_web'} = $web;

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
                editby   => 'TOPICINFO.author',
            );

            my $queryAttrs = {};
            my $orderBy = $sortKeys{ $options->{order} || 'topic' };
            if ( defined($orderBy) ) {
                $queryAttrs = { sort_by => { $orderBy => $SortDirection } };
            }
            else {
                if ( $options->{order} =~ /formfield\((.*)\)/ ) {

#TODO: this will crash things - I need to work on indexes, and one collection per web/form_def
#                    $orderBy = 'FIELD.' . $1.'value';
#                    $queryAttrs = { sort_by => { $orderBy => $SortDirection } };
                }
            }

            my $cursor =
              doMongoSearch( $web, $options, $mongoQuery, $queryAttrs );
            return new Foswiki::Search::MongoDBInfoCache(
                $Foswiki::Plugins::SESSION,
                $web, $options, $cursor );
        }
    }

    #fall back to HoistRe
    require Foswiki::Query::HoistREs;
    my $hoistedREs = Foswiki::Query::HoistREs::collatedHoist($query);

    if (    ( !defined( $options->{topic} ) )
        and ( $hoistedREs->{name} ) )
    {

        #set the 'includetopic' matcher..
        #dammit, i have to de-regex it? thats mad.
    }

    #TODO: howto ask iterator for list length?
    #TODO: once the inputTopicSet isa ResultSet we might have an idea
    #    if ( scalar(@$topics) > 6 ) {
    if ( defined( $hoistedREs->{text} ) ) {
        my $searchOptions = {
            type                => 'regex',
            casesensitive       => 1,
            files_without_match => 1,
        };
        my @filter = @{ $hoistedREs->{text} };
        my $searchQuery =
          new Foswiki::Search::Node( $query->toString(), \@filter,
            $searchOptions );
        $topicSet->reset();

#for now we're kicking down to regex to reduce the set we then brute force query.
#next itr we start to HoistMongoDB
        $topicSet =
          Foswiki::Store::SearchAlgorithms::MongoDB::_webQuery( $searchQuery,
            $web, $topicSet, $session, $searchOptions );
    }
    else {

#TODO: clearly _this_ can be re-written as a FilterIterator, and if we are able to use the sorting hints (ie DB Store) can propogate all the way to FORMAT

        #        print STDERR "WARNING: couldn't hoistREs on " . Dumper($query);
    }

    #print STDERR "))))".$query->toString()."((((\n";
    #    print STDERR "--------Query::MongoDB \n" . Dumper($query) . "\n";
    my $resultTopicSet =
      new Foswiki::Search::InfoCache( $Foswiki::Plugins::SESSION, $web );
    local $/;
    while ( $topicSet->hasNext() ) {
        my $webtopic = $topicSet->next();
        my ( $Iweb, $topic ) =
          Foswiki::Func::normalizeWebTopicName( $web, $webtopic );

#my $meta = Foswiki::Meta->new( $session, $web, $topic );
#GRIN: curiously quick hack to use the MongoDB topics rather than from disk - should have no positive effect on performance :)
#TODO: will make a Store backend later.
        my $meta =
          Foswiki::Plugins::MongoDBPlugin::Meta->new( $session, $web, $topic );

        # this 'lazy load' will become useful when @$topics becomes
        # an infoCache

        # SMELL: CDot modified this without really understanding how
        # it's supposed to work. Once loaded, Meta objects are locked to
        # a specific revision of the topic; it's not clear if the metacache
        # is intended to include different revisions of the same topic
        # or not. See BruteForce.pm for analagous code.
        $meta->loadVersion() unless ( $meta->getLoadedRev() );
        print STDERR "Processing $topic\n"
          if ( Foswiki::Query::Node::MONITOR_EVAL() );
        next unless ( $meta->getLoadedRev() );
        my $match = $query->evaluate( tom => $meta, data => $meta );
        if ($match) {
            $resultTopicSet->addTopic($meta);
        }
    }

    return $resultTopicSet;
}

sub doMongoSearch {
    my $web        = shift;
    my $options    = shift;
    my $ixhQuery   = shift;
    my $queryAttrs = shift;

    #print STDERR "######## Query::MongoDB search ($web)  \n";
    print STDERR "querying mongo: "
      . Dumper($ixhQuery) . " , "
      . Dumper($queryAttrs) . "\n";

#    my $collection =
#      Foswiki::Plugins::MongoDBPlugin::getMongoDB()->_getCollection('current');
#    my $cursor = $collection->query($ixhQuery, $queryAttrs);
    my $cursor = Foswiki::Plugins::MongoDBPlugin::getMongoDB()
      ->query( 'current', $ixhQuery, $queryAttrs );

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

# The getField function is here to allow for Store specific optimisations
# such as direct database lookups.
sub getField {
    my ( $this, $node, $data, $field ) = @_;

    my $result;
    if ( UNIVERSAL::isa( $data, 'Foswiki::Meta' ) ) {

        # The object being indexed is a Foswiki::Meta object, so
        # we have to use a different approach to treating it
        # as an associative array. The first thing to do is to
        # apply our "alias" shortcuts.
        my $realField = $field;
        if ( $Foswiki::Query::Node::aliases{$field} ) {
            $realField = $Foswiki::Query::Node::aliases{$field};
        }
        if ( $realField =~ s/^META:// ) {
            if ( $Foswiki::Query::Node::isArrayType{$realField} ) {

                # Array type, have to use find
                my @e = $data->find($realField);
                $result = \@e;
            }
            else {
                $result = $data->get($realField);
            }
        }
        elsif ( $realField eq 'name' ) {

            # Special accessor to compensate for lack of a topic
            # name anywhere in the saved fields of meta
            return $data->topic();
        }
        elsif ( $realField eq 'text' ) {

            # Special accessor to compensate for lack of the topic text
            # name anywhere in the saved fields of meta
            return $data->text();
        }
        elsif ( $realField eq 'web' ) {

            # Special accessor to compensate for lack of a web
            # name anywhere in the saved fields of meta
            return $data->web();
        }
        elsif ( $realField eq 'hash' ) {

            #return the topic object.
            return $data;
        }
        else {

            # The field name isn't an alias, check to see if it's
            # the form name
            my $form = $data->get('FORM');
            if ( $form && $field eq $form->{name} ) {

                # SHORTCUT;it's the form name, so give me the fields
                # as if the 'field' keyword had been used.
                # TODO: This is where multiple form support needs to reside.
                # Return the array of FIELD for further indexing.
                my @e = $data->find('FIELD');
                return \@e;
            }
            else {

                # SHORTCUT; not a predefined name; assume it's a field
                # 'name' instead.
                # SMELL: Needs to error out if there are multiple forms -
                # or perhaps have a heuristic that gives access to the
                # uniquely named field.
                $result = $data->get( 'FIELD', $field );
                $result = $result->{value} if $result;
            }
        }
    }
    elsif ( ref($data) eq 'ARRAY' ) {

        # Array objects are returned during evaluation, e.g. when
        # a subset of an array is matched for further processing.

        # Indexing an array object. The index will be one of:
        # 1. An integer, which is an implicit index='x' query
        # 2. A name, which is an implicit name='x' query
        if ( $field =~ /^\d+$/ ) {

            # Integer index
            $result = $data->[$field];
        }
        else {

            # String index
            my @res;

            # Get all array entries that match the field
            foreach my $f (@$data) {
                my $val = getField( undef, $node, $f, $field );
                push( @res, $val ) if defined($val);
            }
            if ( scalar(@res) ) {
                $result = \@res;
            }
            else {

                # The field name wasn't explicitly seen in any of the records.
                # Try again, this time matching 'name' and returning 'value'
                foreach my $f (@$data) {
                    next unless ref($f) eq 'HASH';
                    if (   $f->{name}
                        && $f->{name} eq $field
                        && defined $f->{value} )
                    {
                        push( @res, $f->{value} );
                    }
                }
                if ( scalar(@res) ) {
                    $result = \@res;
                }
            }
        }
    }
    elsif ( ref($data) eq 'HASH' ) {

        # A hash object may be returned when a sub-object of a Foswiki::Meta
        # object has been matched.
        $result = $data->{ $node->{params}[0] };
    }
    else {
        $result = $node->{params}[0];
    }
    return $result;
}

# Get a referenced topic
# See Foswiki::Store::QueryAlgorithms.pm for details
sub getRefTopic {
    my ( $this, $relativeTo, $w, $t ) = @_;
    return Foswiki::Meta->load( $relativeTo->session, $w, $t );
}

1;
__END__
This copyright information applies to the MongoDBPlugin:

# Plugin for Foswiki - The Free and Open Source Wiki, http://foswiki.org/
#
# Copyright 2010 - SvenDowideit@fosiki.com
#
# MongoDBPlugin is # This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# For licensing info read LICENSE file in the root of this distribution.
