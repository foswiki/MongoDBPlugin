# See bottom of file for license and copyright information
package Foswiki::Plugins::MongoDBPlugin::MongoDBStore;

use Foswiki::Plugins::MongoDBPlugin qw(writeDebug);
use Foswiki::Plugins::MongoDBPlugin::Meta ();
use Foswiki::Search                       ();
use Foswiki::Func                         ();
use Assert;

use constant MONITOR => 0;

=begin TML

---+ package Foswiki::Plugins::MongoDBPlugin::MongoDBStore;

see F::P::MongoDBPlugin

This class nees to be added to the $Foswiki::cfg{Store}{ImplementationClasses} list.

=cut

sub new {
    my $class = shift;

    my $self = $class->SUPER::new(@_);

    #disable the plugin handler's attempts to keep the mongoDB in sync
    $Foswiki::cfg{Plugins}{MongoDBPlugin}{EnableOnSaveUpdates} = 0;
    $Foswiki::Plugins::MongoDBPlugin::enableOnSaveUpdates = 0;

    writeDebug(
"***************************************MongoDB Listening****************************"
    ) if MONITOR;

    return $self;
}

#TODO: need to have a way for the plugin to disable it
sub enabled {
    my $this = shift;
    return 0 if ($Foswiki::Plugins::MongoDBPlugin::disableMongoDBStoreFilter);
    return 1;
}

=begin TML

---++ ObjectMethod recordChange(%args)
Record that the store item changed, and who changed it

This is a private method to be called only from the store internals, but it can be used by 
$Foswiki::Cfg{Store}{ImplementationClasses} to chain in to eveavesdrop on Store events

        cuid          => $cUID,
        revision      => $rev,
        verb          => $verb,
        newmeta       => $topicObject,
        newattachment => $name

=cut

sub recordChange {
    my ( $this, %args ) = @_;

    #doing it first to make sure the recod is chained
    $this->SUPER::recordChange(%args);

    #TODO: I'm not doing attachments yet
    return if ( defined( $args{newattachment} ) );
    return if ( defined( $args{oldattachment} ) );

    writeDebug( $args{verb} . join( ',', keys(%args) ) ) if MONITOR;

    if ( $args{verb} eq 'remove' ) {

        #works for topics and webs
        Foswiki::Plugins::MongoDBPlugin::_remove( $args{oldmeta}->web,
            $args{oldmeta}->topic );
    }
    elsif ( $args{verb} eq 'insert' ) {

#creating a new web... so we need to add the js we use to get foswiki style functionality
        Foswiki::Plugins::MongoDBPlugin::_updateDatabase(
            $args{newmeta}->{_session},
            $args{newmeta}->web )
          if ( $args{newmeta}->topic eq 'WebPreferences' );

        Foswiki::Plugins::MongoDBPlugin::_updateTopic( $args{newmeta}->web,
            $args{newmeta}->topic, $args{newmeta} );
    }
    elsif ( $args{verb} eq 'update' ) {

        #TODO: not doing web create/move etc yet
        if ( not defined( $args{newmeta}->topic ) ) {

            if ( defined( $args{oldmeta} ) ) {
                if ( $args{oldmeta}->web ne $args{newmeta}->web ) {
                    $self->remove( oldmeta => $args{oldmeta} );
                    writeDebug( "Removed web (" . $args{oldmeta}->web . ")" )
                      if MONITOR;

                    #Force a full scan from filesystem
                    writeDebug( "Scan new web (" . $args{newmeta}->web . ")" )
                      if MONITOR;
                    Foswiki::Plugins::MongoDBPlugin::updateWebCache(
                        $args{newmeta}->web );

                    return;
                }
                writeDebug(
"1. Not sure how we got to this point in updating the Store",
                    -1
                );
            }
            else {
                writeDebug(
"2. Not sure how we got to this point in updating the Store",
                    -1
                );
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
    else {
        writeDebug("UNHANDLED store change");
    }
}

=pod

#######################################################################
#delete these
---++ ObjectMethod readTopic($meta, $version) -> ($gotRev, $isLatest)

NOTE: atm, this will only get called if the Store says yes, this meta item exists on disk
    so we can't inject new topics, only corrupt existing ones

=cut

sub readTopic {

    #    my $self    = shift;
    #    my $_[1]    = shift;
    #    my $_[2] = shift;

    my ( $gotRev, $isLatest );

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
    if ( $_[0]->enabled() && ( not defined($attachment) ) ) {

        if (
            Foswiki::Plugins::MongoDBPlugin::getMongoDB->databaseExists(
                $_[1]->{_web}
            )
          )
        {

            if ( defined( $_[2] ) ) {
                writeDebug("============ listener request for $_[2]")
                  if MONITOR;

                #query the versions collection - via  MongoDBPlugin::Meta
                #rebless into a mighter version of Meta
                bless( $_[1], 'Foswiki::Plugins::MongoDBPlugin::Meta' );
                $_[1]->reload( $_[2] );    #get the requested version
                ( $gotRev, $isLatest ) = ( $_[1]->getLoadedRev(), 1 );
            }
            elsif (
                $session->search->metacache->hasCached(
                    $_[1]->web, $_[1]->topic
                )
              )
            {
            #bugger, infinite loop time
            #writeDebug( "===== metacache hasCached("
            #      . $_[1]->web . " , "
            #      . $_[1]->topic
            #      . ", version)" )
            #  if MONITOR;
            #$_[1] =
            #  $session->search->metacache->getMeta( $_[1]->web, $_[1]->topic );
            #($gotRev, $isLatest) = ( $_[1]->getLoadedRev(), 1 );
            }
            else {
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

                    writeDebug("------ rebless to $metaClass") if MONITOR;
                }

                #cache the metaObj
                $session->search->metacache->addMeta( $_[1]->web, $_[1]->topic,
                    $_[1] );

                writeDebug( "===== loadTopic("
                      . $_[1]->web . " , "
                      . $_[1]->topic
                      . ", version)  => "
                      . $_[1]->getLoadedRev() )
                  if MONITOR;

                ( $gotRev, $isLatest ) = ( $_[1]->getLoadedRev(), 1 );
            }
        }
    }

    if ( defined($gotRev) ) {
        return ( $gotRev, $isLatest );
    }
    else {
        #chain on to other caches and real store
        return $_[0]->SUPER::readTopic( $_[1], $_[2] );
    }
}

sub getRevisionHistory {
    my $this       = shift;
    my $meta       = shift;
    my $attachment = shift;

    my $itr;

#allow the MongoDBPlugin to disable the listener when running a web update resthandler
    if ( $this->enabled() && ( not defined($attachment) ) ) {

        my $session = $meta
          ->{_session}; #TODO: naughty, but we seem to get called before Foswiki::Func::SESSION is set up :(

        if ( $this->{_latestIsLoaded} ) {

            #why poke around in revision history (slow) if we 'have the latest'
            use Foswiki::Iterator::NumberRangeIterator;
            $itr =
              new Foswiki::Iterator::NumberRangeIterator( $this->{_loadedRev},
                1 );
        }
        elsif (
            $session->search->metacache->hasCached( $meta->web, $meta->topic ) )
        {
            writeDebug( "===== metacache hasCached("
                  . $meta->web . " , "
                  . $meta->topic
                  . ", version)" )
              if MONITOR;

            # don't change the version of the loaded meta
            my $cachedmeta =
              $session->search->metacache->getMeta( $meta->web, $meta->topic );
            if ( $cachedmeta->{_latestIsLoaded} ) {

             #why poke around in revision history (slow) if we 'have the latest'
                use Foswiki::Iterator::NumberRangeIterator;
                $itr = new Foswiki::Iterator::NumberRangeIterator(
                    $cachedmeta->{_loadedRev}, 1 );
            }
        }
    }

    if ( defined($itr) ) {
        return $itr;
    }
    else {
        #chain on to other caches and real store
        return $this->SUPER::getRevisionHistory( $meta, $attachment );
    }
}

# SMELL: Store::VC::Store->getVersionInfo doesn't use $rev or $attachment
sub getVersionInfo {
    my ( $this, $topicObject, $rev, $attachment ) = @_;
    my $info;

#allow the MongoDBPlugin to disable the listener when running a web update resthandler
    if ( $this->enabled() && ( not defined($attachment) ) ) {

#TODO: naughty, but we seem to get called before Foswiki::Func::SESSION is set up :(
        my $session = $meta->{_session};

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
        if (MONITOR) {
            require Data::Dumper;
            writeDebug( "MongoDBPlugin::getVersionInfo() GOT: "
                  . Data::Dumper->Dump( [$info] ) );
        }
    }

    if ( defined($info) ) {
        return $info;
    }
    else {
        #chain on to other caches and real store
        return $this->SUPER::getVersionInfo( $topicObject, $rev, $attachment );
    }
}

1;
__DATA__

Author: Sven Dowideit http://fosiki.com

Module of Foswiki - The Free and Open Source Wiki, http://foswiki.org/, http://Foswiki.org/

Copyright (C) 2010 - 2012 Sven Dowideit. All Rights Reserved.

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 3
of the License, or (at your option) any later version. For
more details read LICENSE in the root of this distribution.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

As per the GPL, removal of this notice is prohibited.
