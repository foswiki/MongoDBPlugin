# Plugin for Foswiki - The Free and Open Source Wiki, http://foswiki.org/
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 3
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details, published at
# http://www.gnu.org/copyleft/gpl.html

=pod

---+ package Foswiki::Plugins::MongoDBPlugin


=cut

package Foswiki::Plugins::MongoDBPlugin;

# Always use strict to enforce variable scoping
use strict;

use Foswiki::Func    ();    # The plugins API
use Foswiki::Plugins ();    # For the API version
use Data::Dumper;

our $VERSION = '$Rev: 5771 $';
our $RELEASE = '1.1.1';
our $SHORTDESCRIPTION =
'MongoDB is a scalable, high-performance, open source, schema-free, document-oriented database. ';
our $NO_PREFS_IN_TOPIC = 1;
our $pluginName        = 'MongoDBPlugin';

#this can also be disabled by the listener
our $enableOnSaveUpdates = 0;

=begin TML

---++ initPlugin($topic, $web, $user) -> $boolean
   * =$topic= - the name of the topic in the current CGI query
   * =$web= - the name of the web in the current CGI query
   * =$user= - the login name of the user
   * =$installWeb= - the name of the web the plugin topic is in
     (usually the same as =$Foswiki::cfg{SystemWebName}=)


=cut

sub initPlugin {
    my ( $topic, $web, $user, $installWeb ) = @_;

    $enableOnSaveUpdates =
      $Foswiki::cfg{Plugins}{$pluginName}{EnableOnSaveUpdates} || 0;

    #SMELL: ew
    #TODO: this sets our Global Connextion into the session :(
    getMongoDB();

    Foswiki::Func::registerTagHandler( 'MONGODB', \&_MONGODB );
    Foswiki::Func::registerRESTHandler( 'update', \&_update );
    Foswiki::Func::registerRESTHandler( 'updateDatabase', \&_updateDatabase );

    return 1;
}

=begin TML

---++ completePageHandler($html, $httpHeaders)

add another Header element to simplify performance testing

=cut

sub completePageHandler {

    #    my( $html, $httpHeaders ) = @_;
    #    # modify $_[0] or $_[1] if you must change the HTML or headers
    #    # You can work on $html and $httpHeaders in place by using the
    #    # special perl variables $_[0] and $_[1]. These allow you to operate
    #    # on parameters as if they were passed by reference; for example:
    #    # $_[0] =~ s/SpecialString/my alternative/ge;
    my $queryTimes = 'noQuery';
    my $timeArray  = getMongoDB()->{lastQueryTime};
    $queryTimes = join( ', ', @$timeArray ) if ( defined($timeArray) );
    $Foswiki::Plugins::SESSION->{response}
      ->pushHeader( 'X-Foswiki-Monitor-MongoDBPlugin-lastQueryTime',
        $queryTimes );

}

sub afterSaveHandler {
    return
      if ( $enableOnSaveUpdates != 1 )
      ;    #disabled - they can make save's take too long

    my ( $text, $topic, $web, $error, $meta ) = @_;

    #print STDERR "afterSaveHandler ( text, $topic, $web, error )\n";
    _updateTopic( $web, $topic, $meta );

    return;
}

#mmmm
sub DISABLED_afterRenameHandler {
    return
      if ( $enableOnSaveUpdates != 1 )
      ;    #disabled - they can make save's take too long

    my ( $oldWeb, $oldTopic, $oldAttachment, $newWeb, $newTopic,
        $newAttachment ) = @_;

    print STDERR
"afterRenameHandler: ( $oldWeb, $oldTopic, $oldAttachment, $newWeb, $newTopic,
        $newAttachment )\n";

    #eturn getMongoDB()->rename();
}

sub DISABLED_afterAttachmentSaveHandler {
    return
      if ( $enableOnSaveUpdates != 1 )
      ;    #disabled - they can make save's take too long

    my ( $attrHashRef, $topic, $web ) = @_;

    return getMongoDB()->updateAttachment();
}

################################################################################################################
sub getMongoDB {
    if ( not defined( $Foswiki::Func::SESSION->{MongoDB} ) ) {
        require Foswiki::Plugins::MongoDBPlugin::DB;
        my $mongoDB = new Foswiki::Plugins::MongoDBPlugin::DB(
            {
                host => $Foswiki::cfg{MongoDBPlugin}{host}
                  || 'quad.home.org.au',
                port => $Foswiki::cfg{MongoDBPlugin}{port} || '27017',
                username => $Foswiki::cfg{MongoDBPlugin}{username},
                password => $Foswiki::cfg{MongoDBPlugin}{password},
                database => $Foswiki::cfg{MongoDBPlugin}{database} || 'foswiki',
            }
        );
    }
    return $Foswiki::Func::SESSION->{MongoDB};
}

#restHandler used to update the requested web.
sub _update {
    my $session = shift;
    my $query   = Foswiki::Func::getCgiQuery();
    my $web     = $query->param('updateweb') || 'Sandbox';
    
    #we need to deactivate any listeners :/ () at least stop the loadTopic one from triggering
    $Foswiki::cfg{Store}{Listeners}{'Foswiki::Plugins::MongoDBPlugin::Listener'} = 0; 

    
    #lets make sure we have the javascript we'll rely on later
    _updateDatabase($session, $query);

    my @topicList = Foswiki::Func::getTopicList($web);

    my $count = 0;
    foreach my $topic (@topicList) {
        my ( $meta, $text, $raw_text )
          ;    # = Foswiki::Func::readTopic( $web, $topic );
        my $filename =
          join( '/', ( $Foswiki::cfg{PubDir}, $web, $topic . '.txt' ) );
        if (
            ( 1 == 2 )
            and (
                (
                    $Foswiki::cfg{Store}{Implementation} eq
                    'Foswiki::Store::RcsWrap'
                )
                or ( $Foswiki::cfg{Store}{Implementation} eq
                    'Foswiki::Store::RcsLite' )
            )
            and ( -e $filename )
          )
        {

#if this happens to be a normal file based store, then we can speed things up a bit by breaking the Store abstraction
            $raw_text = Foswiki::Func::readFile($filename);
            $raw_text =~ s/\r//g;    # Remove carriage returns
            $meta->setEmbeddedStoreForm($raw_text);
            $text = $meta->text();
        }
        else {
            ( $meta, $text ) = Foswiki::Func::readTopic( $web, $topic );
        }

        #TODO: listener called for webs too.. (delete, move etc)
        _updateTopic( $web, $topic, $meta, $raw_text );

        $count++;
    }

    return $count;
}

sub _removeTopic {
    my $web   = shift;
    my $topic = shift;

    my $query = { '_web' => $web };
    $query->{'_topic'} = $topic if ( defined($topic) );

    #    $query->{'_attachment'} = $topic if (defined($attachment));

    my $ret = getMongoDB()->remove( 'current', $query );

}

sub _updateTopic {
    my $web       = shift;
    my $topic     = shift;
    my $savedMeta = shift;
    my $raw_text  = shift
      ; #if we already have the embeddedStoreForm store form, we can avoid re-serialising.

    #print STDERR "-update($web, $topic)\n";

    my $meta = {
        _web   => $web,
        _topic => $topic
    };

    foreach my $key ( keys(%$savedMeta) ) {
        #print STDERR "------------------ importing $key - "
        #  . ref( $savedMeta->{$key} ) . "\n";
        next if ( $key eq '_session' );

        #not totally sure if there's a benefit to using / not the _indices
	#TODO: but if I don't use it here, I need to re-create it when loading Meta (and I'm not yet doing that)
        next if ( $key eq '_indices' );

#TODO: as of Oct 2010, mongodb can't sort on an element in an array, so we de-array the ARRAYs.
#TODO: use the registered list of META elements, and the type that is registered.
        if ( $Foswiki::Meta::isArrayType{$key} ) {
            #print STDERR "---- $key == many\n";
            my $FIELD = $savedMeta->{$key};
            $meta->{$key} = {'__RAW_ARRAY' => $FIELD};

            foreach my $elem (@$FIELD) {
                if ( $key eq 'FIELD' ) {

#TODO: move this into the search algo, so it makes an index the first time someone builds an app that sorts on it.
#even then, we have a hard limit of 40 indexes, so we're going to have to get more creative.
#mind you, we don't really need indexes for speed, just to cope with query() resultsets that contain more than 1Meg of documents - so maybe we can delay creation until that happens?
                    getMongoDB()->ensureIndex(
                        'current',
                        { $key . '.' . $elem->{name} => 1 },
                        { name => $key . '.' . $elem->{name} }
                    );
                }

                $meta->{$key}{ $elem->{name} } = $elem;
            }
        }
        else {
            if ( ref( $savedMeta->{$key} ) eq '' ) {
                #print STDERR "-A---$key - " . ref( $savedMeta->{$key} ) . "\n";
                $meta->{$key} = $savedMeta->{$key};
            }
            else {
                if ($key eq '_indices') {
                    #print STDERR "-indicies---($web . $topic) $key\n";
                    $meta->{$key} = $savedMeta->{$key};
                    next;
                }
                if ( ref( $savedMeta->{$key} ) ne 'ARRAY' ) {

  #i don't know why, but this is never triggered, but without it, i get a crash.
  #so, i presume there is a weird case where it happens
                    #print STDERR "-B---($web . $topic) $key - "
                    #  . ref( $savedMeta->{$key} ) . "\n";

#print STDERR Dumper($savedMeta->{$key})."\n";
#print STDERR "\n######################################################## BOOOOOOOOM\n";
                    next;
                }
                #shorcut version of the foreach below because atm, we know there is only one element in the array.
                $meta->{$key} = $savedMeta->{$key}[0];

                #print STDERR "-C---($web . $topic) $key - ";
                #print STDERR Dumper( $meta->{$key}) . "\n";

                if ( $key eq 'TOPICINFO' ) {

#TODO: foswiki's sort by TOPICINFO.author sorts by WikiName, not CUID - so need to make an internal version of this
# to support sort=editby => 'TOPICINFO._authorWikiName',
                    $meta->{$key}->{_authorWikiName} =
                      Foswiki::Func::getWikiName( $meta->{$key}->{author} );
                }
            }
        }
    }

    $meta->{_raw_text} = $raw_text || $savedMeta->getEmbeddedStoreForm();

    my $ret = getMongoDB()->update( 'current', "$web.$topic", $meta );
}

#restHandler used to update the javascript saved in MongoDB
sub _updateDatabase {
    my $session = shift;
    my $query   = Foswiki::Func::getCgiQuery();
    
    #load from topic..
    my $meta = Foswiki::Func::readTopic('Sandbox', 'TestParseTime');
    
    Foswiki::Func::loadTemplate('mongodb_js');
    my $foswiki_d2n_js = Foswiki::Func::expandTemplate('foswiki_d2n_js');
    getMongoDB()->updateSystemJS('foswiki_d2n', $foswiki_d2n_js);
    
    my $foswiki_getRef_js = Foswiki::Func::expandTemplate('foswiki_getRef_js');
    getMongoDB()->updateSystemJS('foswiki_getRef', $foswiki_getRef_js);

    my $foswiki_getField_js = Foswiki::Func::expandTemplate('foswiki_getField_js');
    getMongoDB()->updateSystemJS('foswiki_getField', $foswiki_getField_js);
    
    my $foswiki_toLowerCase_js = Foswiki::Func::expandTemplate('foswiki_toLowerCase_js');
    getMongoDB()->updateSystemJS('foswiki_toLowerCase', $foswiki_toLowerCase_js);
    
    my $foswiki_toUpperCase_js = Foswiki::Func::expandTemplate('foswiki_toUpperCase_js');
    getMongoDB()->updateSystemJS('foswiki_toUpperCase', $foswiki_toUpperCase_js);
    
    my $foswiki_length_js = Foswiki::Func::expandTemplate('foswiki_length_js');
    getMongoDB()->updateSystemJS('foswiki_length', $foswiki_length_js);

    my $foswiki_normaliseTopic_js = Foswiki::Func::expandTemplate('foswiki_normaliseTopic_js');
    getMongoDB()->updateSystemJS('foswiki_normaliseTopic', $foswiki_normaliseTopic_js);
}


# The function used to handle the %EXAMPLETAG{...}% macro
# You would have one of these for each macro you want to process.
sub _MONGODB {
    my ( $session, $params, $theTopic, $theWeb ) = @_;

 #    # $session  - a reference to the Foswiki session object (if you don't know
 #    #             what this is, just ignore it)
 #    # $params=  - a reference to a Foswiki::Attrs object containing
 #    #             parameters.
 #    #             This can be used as a simple hash that maps parameter names
 #    #             to values, with _DEFAULT being the name for the default
 #    #             (unnamed) parameter.
 #    # $theTopic - name of the topic in the query
 #    # $theWeb   - name of the web in the query
 #    # Return: the result of processing the macro. This will replace the
 #    # macro call in the final text.
 #
 #    # For example, %EXAMPLETAG{'hamburger' sideorder="onions"}%
 #    # $params->{_DEFAULT} will be 'hamburger'
 #    # $params->{sideorder} will be 'onions'

    return getMongoDB()->_MONGODB(
        {
            web => 'Sandbox',

            #SMELL: ok, so i'm passing all sorts of stuff
            %$params    #over-ride the defaults
        }
    );
}

# getListOfWebs was moved after 1.1, see Item9814. Should it use Foswiki::Func?
sub _getListOfWebs {
    my ( $webNames, $recurse, $searchAllFlag ) = @_;

    if ( defined &Foswiki::Search::InfoCache::_getListOfWebs ) {

        # Foswiki 1.1
        return Foswiki::Search::InfoCache::_getListOfWebs( $webNames, $recurse,
            $searchAllFlag );
    }
    else {
        require Foswiki::Store::Interfaces::SearchAlgorithm;
        return Foswiki::Store::Interfaces::SearchAlgorithm::getListOfWebs(
            $webNames, $recurse, $searchAllFlag );
    }
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
