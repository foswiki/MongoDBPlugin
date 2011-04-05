# See bottom of file for license and copyright information
package Foswiki::Search::MongoDBInfoCache;
use strict;

use Foswiki::Iterator ();
our @ISA = ('Foswiki::Iterator');

=begin TML

---+ package Foswiki::Search::MongoDBInfoCache

uses the MongoDB cursor, and sorting limiting etc to create a fastpath iterator.

=cut

use Assert;
use Foswiki::Meta                     ();
use Foswiki::Iterator::FilterIterator ();

#use Monitor ();
#Monitor::MonitorMethod('Foswiki::Search::MongoDBInfoCache', 'getTopicListIterator');

=pod
---++ Foswiki::Search::MongoDBInfoCache::new($session, $defaultWeb, $options, $cursor)

=cut

sub new {
    my ( $class, $session, $defaultWeb, $options, $cursor ) = @_;

    #my $this = $class->SUPER::new({});
    my $this = bless( {}, $class );
    $this->{_session}       = $session;
    $this->{_defaultWeb}    = $defaultWeb;
    $this->{_SEARCHoptions} = $options;
    $this->{_cursor}        = $cursor;
    
    return $this;
}

sub numberOfTopics {
    my $this = shift;

    return $this->{cachedCount} if (defined($this->{cachedCount}));
    #count(1) takes into account the skip and limit settings
    #TODO: make sure that this is what we want..
    my $count = 0;#$this->{_cursor}->count(1);
    #TODO: find out if th
    if (($count == 0) and $this->{_cursor}->has_next()) {
	print STDERR "ERROR: cursor count == $count (real_count = ".$this->{_cursor}->{real_count}."), but cursor->has_next is true\n" if DEBUG;
        #work around a bug in MongoDB
        #while ($this->{_cursor}->has_next()) {
        #    $this->{_cursor}->next();
        #    $count++;
        #}
        #$this->{_cursor}->reset();
        $count = $this->{_cursor}->{real_count};
    }
    $this->{cachedCount} = $count;
    return $count;
}

sub hasNext {
    my $this = shift;
    return $this->{_cursor}->has_next;
}

sub next {
    my $this = shift;
    my $obj  = $this->{_cursor}->next;
    my $session = $this->{_session};
    
    if (not ($this->{_cursor}->{noCache})) {
        if (not $session->search->metacache->hasCached( $obj->{_web}, $obj->{_topic})) {
            my $meta = new Foswiki::Plugins::MongoDBPlugin::Meta($session, $obj->{_web}, $obj->{_topic}, $obj);
    #print STDERR "===== MongoDBInfoCache store in metacache (".$meta->web." , ".$meta->topic.", version)\n";
            $session->search->metacache->addMeta( $meta->web, $meta->topic, $meta );
        }
    }
    return $obj->{_web}.'.'.$obj->{_topic};
}
sub reset { return 0; }
sub all   { die 'not implemented'; }

sub isImmutable {
    my $this = shift;
    return 1;
}

sub addTopics {
    my ( $this, $defaultWeb, @list ) = @_;
    ASSERT( !$this->isImmutable() )
      if DEBUG;    #cannot modify list once its being used as an iterator.
    ASSERT( defined($defaultWeb) ) if DEBUG;

    die 'not implemented';
}

#TODO: what if it isa Meta obj
#TODO: or an infoCache obj..
sub addTopic {
    my ( $this, $meta ) = @_;
    ASSERT( !$this->isImmutable() )
      if DEBUG;    #cannot modify list once its being used as an iterator.

    die 'not implemented';
}

=begin TML

---++ sortResults

IMPLEMENTME

=cut

sub sortResults {
    my ( $infoCache, $web, $params ) = @_;
    my $session = $infoCache->{_session};
return; #########################NOP
    my $sortOrder = $params->{order} || '';
    my $revSort   = Foswiki::isTrue( $params->{reverse} );
    my $date      = $params->{date} || '';
    my $limit     = $params->{limit} || '';

    #TODO: sadly, I can't work out a way to sort on 'TOPICINFO[0].date'
    # I have the suspicion that mongodb can't do this directly, so it might
    #have to happen using a javascript function, or i have to abandon
    #the simplicity of using the in-memory data from the Foswiki::Meta obj

    print STDERR "sortResults($sortOrder)\n";

    # sort the topic list by date, author or topic name, and cache the
    # info extracted to do the sorting
    if ( $sortOrder eq 'modified' ) {
        print STDERR "okokokokokokokokok\n";

        #        $infoCache->{_cursor}->sort( {
        #                            'TOPICINFO' => ($revSort?-1:1)
        #                            } );
    }
    elsif (
        $sortOrder =~ /^creat/ ||    # topic creation time
        $sortOrder eq 'editby' ||    # author
        $sortOrder =~ s/^formfield\((.*)\)$/$1/    # form field
      )
    {
        $infoCache->sortTopics( $sortOrder, !$revSort );
    }
    else {

        # simple sort, see Codev.SchwartzianTransformMisused
        # note no extraction of topic info here, as not needed
        # for the sort. Instead it will be read lazily, later on.
        if ($revSort) {
            @{ $infoCache->{list} } =
              sort { $b cmp $a } @{ $infoCache->{list} };
        }
        else {
            @{ $infoCache->{list} } =
              sort { $a cmp $b } @{ $infoCache->{list} };
        }
    }

    if ($date) {
        require Foswiki::Time;
        my @ends       = Foswiki::Time::parseInterval($date);
        my @resultList = ();
        foreach my $topic ( @{ $infoCache->{list} } ) {

            # if date falls out of interval: exclude topic from result
            my $topicdate = $session->getApproxRevTime( $web, $topic );
            push( @resultList, $topic )
              unless ( $topicdate < $ends[0] || $topicdate > $ends[1] );
        }
        @{ $infoCache->{list} } = @resultList;
    }
}

######OLD methods
sub get {
    my ( $this, $topic, $meta ) = @_;

    unless ( $this->{$topic} ) {
        $this->{$topic} = {};
        $this->{$topic}->{tom} = $meta
          || Foswiki::Meta->load( $this->{_session}, $this->{_defaultWeb},
            $topic );

        # SMELL: why do this here? Smells of a hack, as AFAICT it is done
        # anyway during output processing. Disable it, and see what happens....
        #my $text = $topicObject->text();
        #$text =~ s/%WEB%/$web/gs;
        #$text =~ s/%TOPIC%/$topic/gs;
        #$topicObject->text($text);

        # Extract sort fields
        my $ri = $this->{$topic}->{tom}->getRevisionInfo();

        # Rename fields to match sorting criteria
        $this->{$topic}->{editby}   = $ri->{author} || '';
        $this->{$topic}->{modified} = $ri->{date};
        $this->{$topic}->{revNum}   = $ri->{version};

        $this->{$topic}->{allowView} =
          $this->{$topic}->{tom}->haveAccess('VIEW');
    }

    return $this->{$topic};
}

# Determins, and caches, the topic revision info of the base version,
sub getRev1Info {
    my ( $this, $topic, $attr ) = @_;

    my $info = $this->get($topic);
    unless ( defined $info->{$attr} ) {
        my $ri = $info->{rev1info};
        unless ($ri) {
            my $tmp =
              Foswiki::Meta->load( $this->{_session}, $this->{_defaultWeb},
                $topic, 1 );
            $info->{rev1info} = $ri = $tmp->getRevisionInfo();
        }

        if ( $attr eq 'createusername' ) {
            $info->{createusername} =
              $this->{_session}->{users}->getLoginName( $ri->{author} );
        }
        elsif ( $attr eq 'createwikiname' ) {
            $info->{createwikiname} =
              $this->{_session}->{users}->getWikiName( $ri->{author} );
        }
        elsif ( $attr eq 'createwikiusername' ) {
            $info->{createwikiusername} =
              $this->{_session}->{users}->webDotWikiName( $ri->{author} );
        }
        elsif ($attr eq 'createdate'
            or $attr eq 'createlongdate'
            or $attr eq 'created' )
        {
            $info->{created} = $ri->{date};
            require Foswiki::Time;
            $info->{createdate} = Foswiki::Time::formatTime( $ri->{date} );

            #TODO: wow thats disgusting.
            $info->{created} = $info->{createlongdate} = $info->{createdate};
        }
    }
    return $info->{$attr};
}

# Sort a topic list using cached info
sub sortTopics {
    my ( $this, $sortfield, $revSort ) = @_;
    ASSERT($sortfield);

    ASSERT( !$this->isImmutable() )
      ;    #cannot modify list once its being used as an iterator.

    # populate the cache for each topic
    foreach my $topic ( @{ $this->{list} } ) {
        if ( $sortfield =~ /^creat/ ) {

            # The act of getting the info will cache it
            $this->getRev1Info( $topic, $sortfield );
        }
        else {
            my $info = $this->get($topic);
            if ( !defined( $info->{$sortfield} ) ) {
                $info->{$sortfield} =
                  Foswiki::Search::displayFormField( $info->{tom}, $sortfield );
            }
        }

        # SMELL: CDot isn't clear why this is needed, but it is otherwise
        # we end up with the users all being identified as "undef"
        my $info = $this->get($topic);
        $info->{editby} =
          $info->{tom}->session->{users}->getWikiName( $info->{editby} );
    }
    if ($revSort) {
        @{ $this->{list} } = map { $_->[1] }
          sort { _compare( $b->[0], $a->[0] ) }
          map { [ $this->{$_}->{$sortfield}, $_ ] } @{ $this->{list} };
    }
    else {
        @{ $this->{list} } = map { $_->[1] }
          sort { _compare( $a->[0], $b->[0] ) }
          map { [ $this->{$_}->{$sortfield}, $_ ] } @{ $this->{list} };
    }
}

# RE for a full-spec floating-point number
our ($NUMBER);
$NUMBER = qr/^[-+]?[0-9]+(\.[0-9]*)?([Ee][-+]?[0-9]+)?$/s;

sub _compare {
    my $x = shift;
    my $y = shift;
    if ( $x =~ /$NUMBER/o && $y =~ /$NUMBER/o ) {

        # when sorting numbers do it largest first; this is just because
        # this is what date comparisons need.
        return $y <=> $x;
    }
    else {
        return $y cmp $x;
    }
}

#TODO: might use this to optimise for mongoBD regex, or might push to core..
sub convertTopicPatternToRegex {
    my ($topic) = @_;
    return '' unless ($topic);

    # 'Web*, FooBar' ==> ( 'Web*', 'FooBar' ) ==> ( 'Web.*', "FooBar" )
    my @arr =
      map { s/[^\*\_\-\+$Foswiki::regex{mixedAlphaNum}]//go; s/\*/\.\*/go; $_ }
      split( /(?:,\s*|\|)/, $topic );
    return '' unless (@arr);

    # ( 'Web.*', 'FooBar' ) ==> "^(Web.*|FooBar)$"
    return '^(' . join( '|', @arr ) . ')$';
}

1;
__END__

# Plugin for Foswiki - The Free and Open Source Wiki, http://foswiki.org/
#
# Copyright 2010 - SvenDowideit@fosiki.com
#
# MongoDBPlugin is # This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# For licensing info read LICENSE file in the root of this distribution.
