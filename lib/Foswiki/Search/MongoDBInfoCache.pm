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

    return $this->{cachedCount} if ( defined( $this->{cachedCount} ) );

#count(1) takes into account the skip and limit settings
#which is _not_ what we want.. (as the count is used to get the total number of available pages.
    my $count = 0;    #$this->{_cursor}->count();
                      #TODO: find out if th
    if ( ( $count == 0 ) and $this->{_cursor}->has_next() ) {

#print STDERR "ERROR: cursor count == $count (real_count = ".$this->{_cursor}->{real_count}."), but cursor->has_next is true\n";
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
    $this->numberOfTopics()
      if ( not defined( $this->{cachedCount} ) )
      ; #TODO: sadly, the count is wrong once we start iterating, as it returns number remaining
    return $this->{_cursor}->has_next;
}

sub NOskip {

#this is a nop - as the paging must be done much earlier. (this will soooo stuff up the count)
    my $self = shift;

}

sub next {
    my $this    = shift;
    my $obj     = $this->{_cursor}->next;
    my $session = $this->{_session};

    if ( not( $this->{_cursor}->{noCache} ) ) {
        if (
            not $session->search->metacache->hasCached(
                $obj->{_web}, $obj->{_topic}
            )
          )
        {
            my $meta =
              new Foswiki::Plugins::MongoDBPlugin::Meta( $session, $obj->{_web},
                $obj->{_topic}, $obj );

#print STDERR "===== MongoDBInfoCache store in metacache (".$meta->web." , ".$meta->topic.", version)\n";
            $session->search->metacache->addMeta( $meta->web, $meta->topic,
                $meta );
        }
    }

    #print STDERR "next => $obj->{_web}.'.'.$obj->{_topic}\n";
    return $obj->{_web} . '.' . $obj->{_topic};
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

sub sortResults {

    #does nothing, as the results are already sorted in monogDB
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


=begin TML

---++ filterByDate( $date )

Filter the list by date interval; see System.TimeSpecifications.

<verbatim>
$infoCache->filterByDate( $date );
</verbatim>

this can either be implemented using mongodb side things, or as a filterIterator
but as the API does not return a new iterator, its iffy

method here only to prevent foswiki crashes

=cut

sub filterByDate {
    my ( $this, $date ) = @_;

#not implemented.

}


1;
__END__

# Plugin for Foswiki - The Free and Open Source Wiki, http://foswiki.org/
#
# Copyright 2010-2012 - SvenDowideit@fosiki.com
#
# MongoDBPlugin is # This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# For licensing info read LICENSE file in the root of this distribution.
