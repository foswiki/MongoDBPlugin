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
    $Foswiki::cfg{Plugins}{MongoDBPlugin}{Enabled} = 1;
    print STDERR "****** starting MongoDBPlugin..\n" if MONITOR;
    
    $Foswiki::Plugins::SESSION->{store}->setListenerPriority('Foswiki::Plugins::MongoDBPlugin::Listener', 1);
}

use Foswiki::Search::Node ();
use Foswiki::Store::SearchAlgorithms::MongoDB();
use Foswiki::Plugins::MongoDBPlugin       ();
use Foswiki::Plugins::MongoDBPlugin::Meta ();
use Foswiki::Search::InfoCache;
use Foswiki::Plugins::MongoDBPlugin::HoistMongoDB;
use Data::Dumper;
use Assert;

use Foswiki::Query::Node;
use Foswiki::Query::OP_and;
use Foswiki::Infix::Error;

=begin TML

---++ ClassMethod new( $class,  ) -> $cereal

=cut

sub new {
    my $self = shift()->SUPER::new( 'SEARCH', @_ );
    return $self;
}

# Query over a single web
sub _webQuery {
    my ( $this, $query, $web, $inputTopicSet, $session, $options ) = @_;

    #TODO: what happens if / when the inputTopicSet exists?

#presuming that the inputTopicSet is not yet defined, we need to add the topics=, excludetopic= and web options to the query.
    my $extra_query;
    {
        my @option_query = ();
        if ( $options->{topic} ) {
            push( @option_query,
                convertTopicPatternToLonghandQuery( $options->{topic} ) );
        }
        if ( $options->{excludetopic} ) {
            push(
                @option_query,
                'NOT('
                  . convertTopicPatternToLonghandQuery(
                    $options->{excludetopic}
                  )
                  . ')'
            );

#> db.current.find({_web: 'Sandbox',  _topic : {'$nin' :[ /AjaxComment/]}}, {_topic:1})
#> db.current.find({_web: 'Sandbox',  _topic : {'$nin' :[/Web.*/]}}, {_topic:1})

        }
        my $queryStr = join( ' AND ', @option_query );

        #print STDERR "NNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN($queryStr)\n";
        if ( $queryStr eq '' ) {
        }
        else {
            my $theParser = $session->search->{queryParser};
            $extra_query = $theParser->parse( $queryStr, $options );
        }
    }

#SMELL: initialise the mongoDB hack. needed if the mondoPlugin is not enabled, but the algo is selected :/
    Foswiki::Plugins::MongoDBPlugin::getMongoDB();

    if ( $query->evaluatesToConstant() ) {

        # SMELL: use any old topic
        my $cache = $Foswiki::Plugins::SESSION->search->metacache->get( $web,
            'WebPreferences' );
        my $meta = $cache->{tom};
        my $queryIsAConstantFastpath =
          $query->evaluate( tom => $meta, data => $meta );
        if ( not $queryIsAConstantFastpath ) {

            #false - return an empty resultset
            return new Foswiki::Search::InfoCache( $Foswiki::Plugins::SESSION,
                $web );
        }
        else {

    #need to do the query - at least to eval topic= and excludetopic= and order=
            $query = $extra_query;
        }
    }
    else {
        if ( defined($extra_query) ) {
            my $and = new Foswiki::Query::OP_and();
            $query =
              Foswiki::Query::Node->newNode( $and, ( $extra_query, $query ) );
        }
    }

    print STDERR "modified parsetree: "
      . ( defined($query) ? $query->stringify() : 'undef' ) . "\n"
      if MONITOR;

    #try HoistMongoDB first
    my $mongoQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);

    if ( not defined($mongoQuery) ) {
        print STDERR "MongoDB QuerySearch - failed to hoist to MongoDB ("
          . $query->stringify()
          . ") - please report the error to Sven.\n";

        #falling through to old regex code
    }
    else {
        ASSERT( not( defined( $mongoQuery->{ERROR} ) ) ) if DEBUG;
        
        #add ACL filter
        my $userIsIn = Foswiki::Plugins::MongoDBPlugin::getACLProfilesFor($session->{user}, $web);
        ### ((_ACLProfile_ALLOWTOPICVIEW: $in(userIsIn, UNDEF)) AND (_ACLProfile.DENYTOPICVIEW: $NOTin(userIsIn)))
        #TODO: this is incorrect, it needs to also have the logic for the web default (and be inverted if the web DENYs the user..
        if ($session->access->haveAccess('VIEW', $session->{user}, $web)) {
            #TODO: potential BUG - if user is in both allow and deny, the algo chooses allow
            $mongoQuery->{_ACLProfile_ALLOWTOPICVIEW} = {'$in' => [@$userIsIn, 'UNDEFINED']};
            $mongoQuery->{_ACLProfile_DENYTOPICVIEW} = {'$nin' => $userIsIn};
        } else {
            #user is already denied, so we only get view access _if_ the user is specifically ALLOWed
            $mongoQuery->{_ACLProfile_ALLOWTOPICVIEW} = {'$in' => [@$userIsIn]};
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
                        $Foswiki::cfg{Plugins}{MongoDBPlugin}{ExperimentalCode}
                    )
                    and $Foswiki::cfg{Plugins}{MongoDBPlugin}{ExperimentalCode}
                  )
                {
                    $orderBy = 'FIELD.' . $1 . '.value';
                    $queryAttrs = { sort_by => { $orderBy => $SortDirection } };
                }
            }
        }

        #if ($options->{paging_on}) {
        #    $queryAttrs->{skip} = $options->{showpage} * $options->{pagesize};
        #    $queryAttrs->{limit} = $options->{pagesize};
        #}

        my $cursor = doMongoSearch( $web, $options, $mongoQuery, $queryAttrs );
        return new Foswiki::Search::MongoDBInfoCache(
            $Foswiki::Plugins::SESSION,
            $web, $options, $cursor );
    }

    ######################################
    #fall back to HoistRe
    my $topicSet = $inputTopicSet;
    if ( !defined($topicSet) ) {

        #then we start with the whole web?
        #TODO: i'm sure that is a flawed assumption
        my $webObject = Foswiki::Meta->new( $session, $web );
        $topicSet =
          Foswiki::Search::InfoCache::getTopicListIterator( $webObject,
            $options );
    }

    require Foswiki::Query::HoistREs;
    my $hoistedREs = Foswiki::Query::HoistREs::hoist($query);

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

    my $cursor = Foswiki::Plugins::MongoDBPlugin::getMongoDB()
      ->query( $web, 'current', $ixhQuery, $queryAttrs );

    return $cursor;
}

sub convertTopicPatternToLonghandQuery {
    my ($topic) = @_;
    return '' unless ($topic);

    # 'Web*, FooBar' ==> ( 'Web*', 'FooBar' ) ==> ( 'Web.*', "FooBar" )
    my @arr =
      map { s/[^\*\_\-\+$Foswiki::regex{mixedAlphaNum}]//go; s/\*/\.\*/go; $_ }
      split( /(?:,\s*|\|)/, $topic );
    return '' unless (@arr);

    # ( 'Web.*', 'FooBar' ) ==> "^(Web.*|FooBar)$"
    #return '^(' . join( '|', @arr ) . ')$';
    return join(
        ' OR ',
        map {
            if (/\.\*/)
            {
                "name =~ '" . $_ . "'";
            }
            else {
                "name='" . $_ . "'";
            }
          } @arr
    );
}

1;
__END__
This copyright information applies to the MongoDBPlugin:

# Plugin for Foswiki - The Free and Open Source Wiki, http://foswiki.org/
#
# Copyright 2010-2011 - SvenDowideit@fosiki.com
#
# MongoDBPlugin is # This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# For licensing info read LICENSE file in the root of this distribution.
