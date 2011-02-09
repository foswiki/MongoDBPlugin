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

use Foswiki::Store::Interfaces::QueryAlgorithm ();
our @ISA = ('Foswiki::Store::Interfaces::QueryAlgorithm');

use strict;
use constant MONITOR => 0;

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
    {

        #try HoistMongoDB first
        my $mongoQuery =
          Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);

        if ( defined($mongoQuery) ) {
            ASSERT( not( defined( $mongoQuery->{ERROR} ) ) ) if DEBUG;

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
                    if (
                        defined(
                            $Foswiki::cfg{Plugins}{MongoDBPlugin}
                              {ExperimentalCode}
                        )
                        and
                        $Foswiki::cfg{Plugins}{MongoDBPlugin}{ExperimentalCode}
                      )
                    {
                        $orderBy = 'FIELD.' . $1 . '.value';
                        $queryAttrs =
                          { sort_by => { $orderBy => $SortDirection } };
                    }
                }
            }

            my $cursor =
              doMongoSearch( $web, $options, $mongoQuery, $queryAttrs );
            return new Foswiki::Search::MongoDBInfoCache(
                $Foswiki::Plugins::SESSION,
                $web, $options, $cursor );
        } else {
		print STDERR "MongoDB QuerySearch - failed to hoist to MongoDB - please report the error to Sven.\n";
		#falling through to old regex code
	}
    }


    #fall back to HoistRe
    require Foswiki::Query::HoistREs;
    my $hoistedREs = Foswiki::Query::HoistREs::collatedHoist($query);

    if (    ( !defined( $options->{topic} ) )
        and ( $hoistedREs->{name} ) )
    {

        #set the ' includetopic ' matcher..
        #dammit, i have to de-regex it? thats mad.
    }

    #TODO: howto ask iterator for list length?
    #TODO: once the inputTopicSet isa ResultSet we might have an idea
    #    if ( scalar(@$topics) > 6 ) {
    if ( defined( $hoistedREs->{text} ) ) {
        my $searchOptions = {
            type                => ' regex ',
            casesensitive       => 1,
            files_without_match => 1,
        };
        my @filter = @{ $hoistedREs->{text} };
        my $searchQuery =
          new Foswiki::Search::Node( $query->toString(), \@filter,
            $searchOptions );
        $topicSet->reset();

        #for now we're kicking down to regex to reduce the set we then brute force query .

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
    #print STDERR "querying mongo: "
    #  . Dumper($ixhQuery) . " , "
    #  . Dumper($queryAttrs) . "\n";

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
