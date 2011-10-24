# See bottom of file for license and copyright information
package Foswiki::Plugins::MongoDBPlugin::Listener;

use Foswiki::Plugins::MongoDBPlugin       ();
use Foswiki::Plugins::MongoDBPlugin::Meta ();
use Foswiki::Search                       ();
use Foswiki::Func                         ();
use Assert;

use constant MONITOR => 0;

=begin TML

---+ package Foswiki::Plugins::MongoDBPlugin::Listener;

see F::P::MongoDBPlugin
    $Foswiki::Plugins::SESSION->{store}->setListenerPriority('Foswiki::Plugins::MongoDBPlugin::Listener', 1);


=cut

sub new {
    my $class = shift;

    my $self = bless {@_}, $class;

    #disable the plugin handler's attempts to keep the mongoDB in sync
    $Foswiki::cfg{Plugins}{MongoDBPlugin}{EnableOnSaveUpdates} = 0;
    $Foswiki::Plugins::MongoDBPlugin::enableOnSaveUpdates = 0;

    print STDERR
"***************************************MongoDB Listening****************************\n"
      if MONITOR;

    return $self;
}

=begin TML

---++ ObjectMethod insert($metaObject)
Event triggered when a new Meta object is inserted into the store

=cut

sub insert {
    my $self = shift;
    my %args = @_;

    print STDERR "inserting " . join( ',', keys(%args) ) . "\n" if MONITOR;
    print STDERR "     ("
      . $args{newmeta}->web . ", "
      . ( $args{newmeta}->topic || 'UNDEF' ) . ")\n"
      if MONITOR;

    return if ( defined( $args{newattachment} ) );

#creating a new web... so we need to add the js we use to get foswiki style functionality
    Foswiki::Plugins::MongoDBPlugin::_updateDatabase(
        $args{newmeta}->{_session},
        $args{newmeta}->web )
      if ( $args{newmeta}->topic eq 'WebPreferences' );

    Foswiki::Plugins::MongoDBPlugin::_updateTopic( $args{newmeta}->web,
        $args{newmeta}->topic, $args{newmeta} );
}

=begin TML

---++ ObjectMethod update($oldMetaObject[, $newMetaObject])

We are updating the object. This is triggered when a meta-object
is saved. It should be logically equivalent to:
<verbatim>
remove($oldMetaObject)
insert($newMetaObject || $oldMetaObject)
</verbatim>
but listeners may optimise on this. The two parameter form is called when
a topic is moved.

=cut

sub update {
    my $self = shift;
    my %args = @_;

    #TODO: I'm not doing attachments yet
    return if ( defined( $args{newattachment} ) );
    return if ( defined( $args{oldattachment} ) );

    #TODO: not doing web create/move etc yet
    if ( not defined( $args{newmeta}->topic ) ) {

        if ( defined( $args{oldmeta} ) ) {
            if ( $args{oldmeta}->web ne $args{newmeta}->web ) {
                $self->remove( oldmeta => $args{oldmeta} );
                print STDERR "Removed web (" . $args{oldmeta}->web . ")\n"
                  if MONITOR;

                #Force a full scan from filesystem
                print STDERR "Scan new web (" . $args{newmeta}->web . ")\n"
                  if MONITOR;
                Foswiki::Plugins::MongoDBPlugin::updateWebCache(
                    $args{newmeta}->web );

                return;
            }
            print STDERR
              "1. Not sure how we got to this point in updating the Listener\n";
        }
        else {
            print STDERR
              "2. Not sure how we got to this point in updating the Listener\n";
        }

        return;
    }

    #TODO: do this differently when we support previous revs
    if ( defined( $args{oldmeta} ) ) {

        #move topic is (currently) a delete&insert
        $self->remove( oldmeta => $args{oldmeta} );
    }
    if ( defined( $args{newmeta}->topic ) ) {
        Foswiki::Plugins::MongoDBPlugin::_updateTopic( $args{newmeta}->web,
            $args{newmeta}->topic, $args{newmeta} );
    }
}

=begin TML

---++ ObjectMethod remove(oldmeta=>obj [,  oldattachment=>$string])
We are removing the given object.

=cut

sub remove {
    my $self = shift;
    my %args = @_;
    ASSERT( $args{oldmeta} ) if DEBUG;

    #lets not delete the topic if we're actually deleting an attachment
    return if ( defined( $args{oldattachment} ) );

    print STDERR "removing " . join( ',', keys(%args) ) . "\n" if MONITOR;
    print STDERR "     ("
      . $args{oldmeta}->web . ", "
      . ( $args{oldmeta}->topic || 'UNDEF' ) . ")\n"
      if MONITOR;

    #works for topics and webs
    Foswiki::Plugins::MongoDBPlugin::_remove( $args{oldmeta}->web,
        $args{oldmeta}->topic );
}

=begin TML

---++ ObjectMethod loadTopic($meta, $version) -> ($gotRev, $isLatest)

NOTE: atm, this will only get called if the Store says yes, this meta item exists on disk
    so we can't inject new topics, only corrupt existing ones

=cut

sub loadTopic {

    #    my $self    = shift;
    #    my $_[1]    = shift;
    #    my $_[2] = shift;

    my $session =
      $_[1]
      ->{_session}; #TODO: naughty, but we seem to get called before Foswiki::Func::SESSION is set up :(

    $_[0]->{count} = {} unless ( defined( $_[0]->{count} ) );
    $_[0]->{count}{ $_[1]->web } = {}
      unless ( defined( $_[0]->{count}{ $_[1]->web } ) );
    $_[0]->{count}{ $_[1]->web }{ $_[1]->topic } = 0
      unless ( defined( $_[0]->{count}{ $_[1]->web }{ $_[1]->topic } ) );

    $_[0]->{count}{ $_[1]->web }{ $_[1]->topic }++;

#die 'here' if ($_[0]->{count}{$_[1]->web}{$_[1]->topic} > 10); #sometime there is recursion, and this way i can track it down

#allow the MongoDBPlugin to disable the listener when running a web update resthandler
    return
      if (
        not $Foswiki::cfg{Store}{Listeners}
        {'Foswiki::Plugins::MongoDBPlugin::Listener'} );

    #fail faster.
    return
      unless (
        Foswiki::Plugins::MongoDBPlugin::getMongoDB->databaseExists(
            $_[1]->{_web}
        )
      );

    if (
        ( defined( $_[2] ) ) and    #topic versioning in mongodb
        (
            defined( $Foswiki::cfg{Plugins}{MongoDBPlugin}{ExperimentalCode} )
            and $Foswiki::cfg{Plugins}{MongoDBPlugin}{ExperimentalCode}
        )
      )
    {
        print STDERR "============ listener request for $_[2]\n" if MONITOR;

        #return;
        #query the versions collection - via  MongoDBPlugin::Meta
        #rebless into a mighter version of Meta
        bless( $_[1], 'Foswiki::Plugins::MongoDBPlugin::Meta' );
        $_[1]->reload( $_[2] );    #get the requested version
        return ( $_[1]->getLoadedRev(), 1 );
    }

    if ( $session->search->metacache->hasCached( $_[1]->web, $_[1]->topic ) ) {
        return;                    #bugger, infinite loop time
        print STDERR "===== metacache hasCached("
          . $_[1]->web . " , "
          . $_[1]->topic
          . ", version)\n"
          if MONITOR;
        $_[1] =
          $session->search->metacache->getMeta( $_[1]->web, $_[1]->topic );
        return ( $_[1]->getLoadedRev(), 1 );
    }

#rebless into a mighter version of Meta
#TODO: none of this horrid monkeying should be needed
#the proper fix will be to separate (de)serialization from meta and store, so they can be mixin/aspect/something
    my $metaClass = ref( $_[1] );
    bless( $_[1], 'Foswiki::Plugins::MongoDBPlugin::Meta' );
    $_[1]->reload();    #get the latest version
    $_[1]->{_latestIsLoaded} = 1;
    if ( $metaClass ne 'Foswiki::Meta' ) {

        #return us to what we were..
        bless( $_[1], $metaClass );

        print STDERR "------ rebless to $metaClass\n" if MONITOR;
    }

    #cache the metaObj
    $session->search->metacache->addMeta( $_[1]->web, $_[1]->topic, $_[1] );

    print STDERR "===== loadTopic("
      . $_[1]->web . " , "
      . $_[1]->topic
      . ", version)  => "
      . $_[1]->getLoadedRev() . "\n"
      if MONITOR;

    return ( $_[1]->getLoadedRev(), 1 );

}

sub getRevisionHistory {
    my $this       = shift;
    my $meta       = shift;
    my $attachment = shift;

#allow the MongoDBPlugin to disable the listener when running a web update resthandler
    return
      if (
        not $Foswiki::cfg{Store}{Listeners}
        {'Foswiki::Plugins::MongoDBPlugin::Listener'} );

    return if ( defined($attachment) );

    my $session = $meta
      ->{_session}; #TODO: naughty, but we seem to get called before Foswiki::Func::SESSION is set up :(

    if ( ( not defined($attachment) ) and ( $this->{_latestIsLoaded} ) ) {

        #why poke around in revision history (slow) if we 'have the latest'
        use Foswiki::Iterator::NumberRangeIterator;
        return new Foswiki::Iterator::NumberRangeIterator( $this->{_loadedRev},
            1 );
    }

    if ( $session->search->metacache->hasCached( $meta->web, $meta->topic ) ) {
        print STDERR "===== metacache hasCached("
          . $meta->web . " , "
          . $meta->topic
          . ", version)\n"
          if MONITOR;
        $meta =
          $session->search->metacache->getMeta( $meta->web, $meta->topic );
        if ( ( not defined($attachment) ) and ( $meta->{_latestIsLoaded} ) ) {

            #why poke around in revision history (slow) if we 'have the latest'
            use Foswiki::Iterator::NumberRangeIterator;
            return new Foswiki::Iterator::NumberRangeIterator(
                $meta->{_loadedRev}, 1 );
        }
    }

    return undef;
}

# SMELL: Store::VC::Store->getVersionInfo doesn't use $rev or $attachment
sub getVersionInfo {
    my ( $this, $topicObject, $rev, $attachment ) = @_;
    my $info;

#allow the MongoDBPlugin to disable the listener when running a web update resthandler
    return
      if (
        not $Foswiki::cfg{Store}{Listeners}
        {'Foswiki::Plugins::MongoDBPlugin::Listener'} );

#TODO: naughty, but we seem to get called before Foswiki::Func::SESSION is set up :(
    my $session = $meta->{_session};

    if ( not defined($attachment) ) {
        if (    defined $this->{_loadedRev}
            and defined $topicObject->{_loadedRev}
            and $this->{_loadedRev} == $topicObject->{_loadedRev} )
        {
            $info = $topicObject->{'TOPICINFO'};
        }
        else {

            # SMELL: this seems a bit ... circular
            my ($tempObject) =
              Foswiki::Func::readTopic( $topicObject->web(),
                $topicObject->topic(), $rev );
            $info = $tempObject->{'TOPICINFO'};
        }

        if ( defined $info ) {
            ASSERT( ref($info) eq 'ARRAY' and scalar( @{$info} ) == 1 )
              if DEBUG;
            $info = $info->[0];
            ASSERT( ref($info) eq 'HASH' ) if DEBUG;
            ASSERT( scalar( keys %{$info} ) ) if DEBUG;
            $info->{date}    = 0  unless defined $info->{date};
            $info->{version} = 1  unless defined $info->{version};
            $info->{comment} = '' unless defined $info->{comment};
            $info->{author} ||=
              $Foswiki::Users::BaseUserMapping::UNKNOWN_USER_CUID;
        }
    }
    if (MONITOR) {
        require Data::Dumper;
        print STDERR "MongoDBPlugin::getVersionInfo() GOT: "
          . Data::Dumper->Dump( [$info] );
    }

    return $info;
}

1;
__DATA__

Author: Sven Dowideit http://fosiki.com

Module of Foswiki - The Free and Open Source Wiki, http://foswiki.org/, http://Foswiki.org/

Copyright (C) 2010 Foswiki Contributors. All Rights Reserved.
Foswiki Contributors are listed in the AUTHORS file in the root
of this distribution. NOTE: Please extend that file, not this notice.

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 3
of the License, or (at your option) any later version. For
more details read LICENSE in the root of this distribution.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

As per the GPL, removal of this notice is prohibited.
