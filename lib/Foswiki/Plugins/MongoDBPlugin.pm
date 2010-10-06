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

#$debug = $Foswiki::cfg{Plugins}{KinoSearchPlugin}{Debug} || 0;
    $enableOnSaveUpdates = $Foswiki::cfg{Plugins}{$pluginName}{EnableOnSaveUpdates} || 0;

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

    $meta->{_raw_text} = $meta->getEmbeddedStoreForm();

#TODO: WARNING: this needs to be moved up the tree - we're serialising all references in the topic obj, and _session is huge, _indices can contain TOPICPARENT with '' as key
    my $_sess = $meta->{_session};
    my $_indices = $meta->{_indices};
    delete ($meta->{_indices});
    delete ($meta->{_session});

    my $ret = getMongoDB()->update( 'current', "$web.$topic", $meta );
    $meta->{_session} = $_sess;
    $meta->{_indices} = $_indices;

	return $ret;
}

#mmmm
sub DISABLED_afterRenameHandler {
    return
      if ( $enableOnSaveUpdates != 1 )
      ;    #disabled - they can make save's take too long

    my ( $oldWeb, $oldTopic, $oldAttachment, $newWeb, $newTopic,
        $newAttachment ) = @_;

    return getMongoDB()->rename();
}

sub DISABLED_afterAttachmentSaveHandler {
    return
      if ( $enableOnSaveUpdates != 1 )
      ;    #disabled - they can make save's take too long

    my ( $attrHashRef, $topic, $web ) = @_;

    return getMongoDB()->udpateAttachment();
}

################################################################################################################
sub getMongoDB {
    if ( not defined( $Foswiki::Func::SESSION->{MongoDB} ) ) {
        require Foswiki::Plugins::MongoDBPlugin::DB;
        my $mongoDB = new Foswiki::Plugins::MongoDBPlugin::DB(
            {
                host => $Foswiki::cfg{MongoDBPlugin}{host} || 'quad.home.org.au',
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
        
        $meta->{_raw_text} = $meta->getEmbeddedStoreForm();
        getMongoDB()->update( 'current', "$web.$topic", $meta );
        $count++;
    }

    return $count;
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
