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

    return 1;
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

sub _update {
    my $session = shift;
    my $query   = Foswiki::Func::getCgiQuery();
    my $web     = $query->param('updateweb') || 'Sandbox';

    my @topicList = Foswiki::Func::getTopicList($web);

    my $count = 0;
    foreach my $topic (@topicList) {
        my ( $meta, $text ) = Foswiki::Func::readTopic( $web, $topic );

        _updateTopic( $web, $topic, $meta );

        $count++;
    }

    return $count;
}

sub _updateTopic {
    my $web       = shift;
    my $topic     = shift;
    my $savedMeta = shift;

    #print STDERR "-update($web, $topic)\n";

    my $meta = {
        _web   => $web,
        _topic => $topic
    };

    foreach my $key ( keys(%$savedMeta) ) {
        next if ( $key eq '_session' );
        next if ( $key eq '_indices' );

#TODO: as of Oct 2010, mongodb can't sort on an element in an array, so we de-array the ARRAYs.
        if (   ( $key eq 'TOPICINFO' )
            or ( $key eq 'TOPICPARENT' ) )
        {

#shorcut version of the foreach below because atm, we know there is only one element in the array.
            $meta->{$key} = $savedMeta->{$key}[0];
        }
        elsif
          ( #probably should just 'if ARRAY' but that makes it harder to un-array later.
            ( $key    eq 'FILEATTACHMENT' )
            or ( $key eq 'FIELD' )
            or ( $key eq 'PREFERENCE' )
          )
        {
            my $FIELD = $savedMeta->{$key};
            $meta->{$key} = {};
            foreach my $elem (@$FIELD) {
                $meta->{$key}{ $elem->{name} } = $elem;
            }
            next;
        }

    }

    $meta->{_raw_text} = $savedMeta->getEmbeddedStoreForm();

    my $ret = getMongoDB()->update( 'current', "$web.$topic", $meta );
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
