# See bottom of file for license and copyright information

package Foswiki::Store::SearchAlgorithms::MongoDB;

use strict;
use Assert;
use Foswiki::Search::InfoCache;

=begin TML

---+ package Foswiki::Store::SearchAlgorithms::MongoDB

A very simplistic conversion from PurePerl to querying mongoDB

still doing a very dumb iteration through the queries and then aggregating it in perl

also, its currently ignoring the input result set.

---++ search($searchString, $inputTopicSet, $session, $options) -> \%seen
Search .txt files in $dir for $string. See RcsFile::searchInWebContent
for details.

DEPRECATED


=cut

sub search {
    my ( $searchString, $web, $inputTopicSet, $session, $options ) = @_;

    local $/ = "\n";
    my %seen;
    if ( $options->{type} && $options->{type} eq 'regex' ) {

        # Escape /, used as delimiter. This also blocks any attempt to use
        # the search string to execute programs on the server.
        $searchString =~ s!/!\\/!g;
    }
    else {

        # Escape non-word chars in search string for plain text search
        $searchString =~ s/(\W)/\\$1/g;
    }

    # Convert GNU grep \< \> syntax to \b
    $searchString =~ s/(?<!\\)\\[<>]/\\b/g;
    $searchString =~ s/^(.*)$/\\b$1\\b/go if $options->{'wordboundaries'};

    my $cursor =
      doMongoSearch( $web, $options, '_text', $searchString );

    #TODO: this will go into the custom TopicSet
    while ( my $topic = $cursor->next ) {
        $seen{ $topic->{_topic} } = 1;
    }

    return \%seen;
}

=begin TML

this is the new way -

=cut

sub query {
    my ( $query, $web, $inputTopicSet, $session, $options ) = @_;
    ASSERT( scalar( @{ $query->{tokens} } ) > 0 ) if DEBUG;

    # default scope is 'text'
    $options->{'scope'} = 'text'
      unless ( defined( $options->{'scope'} )
        && $options->{'scope'} =~ /^(topic|all)$/ );

    my $topicSet = $inputTopicSet;

    print STDERR "######## Search::MongoDB query ($web) tokens "
      . scalar( @{ $query->{tokens} } ) . " : "
      . join( ',', @{ $query->{tokens} } ) . "\n";

# AND search - search once for each token, ANDing result together
#TODO: this is stupid. suggested re-impl:
#               the query & search functions in the query&search algo just _create_ the hash for the query
#               and this is stored in the topic Set. When the topic set is 'evaluated' the query is sent (by the topic set)
#               and from there the cursor is used.
#nonetheless, the rendering of 2000 results takes much longer than the querying, but as the 2 are on separate servers, everything is golden :)
    foreach my $token ( @{ $query->{tokens} } ) {

        # flag for AND NOT search
        my $invertSearch = 0;
        $invertSearch = ( $token =~ s/^\!//o );

        # scope can be 'topic' (default), 'text' or "all"
        # scope='topic', e.g. Perl search on topic name:
        my %topicMatches;
        unless ( $options->{'scope'} eq 'text' ) {
            my $searchString = $token;

            # FIXME I18N
            if ( $options->{'type'} ne 'regex' ) {
                $searchString = quotemeta($searchString);
            }

            my $cursor =
              doMongoSearch( $web, $options, '_topic', $searchString);

            #TODO: this will go into the custom TopicSet
            while ( my $topic = $cursor->next ) {
                $topicMatches{ $topic->{_topic} } = 1;
            }

        }
        print STDERR "after topic scope search\n";

        # scope='text', e.g. grep search on topic text:
        my $textMatches;
        unless ( $options->{'scope'} eq 'topic' ) {
            $textMatches =
              search( $token, $web, $topicSet, $session->{store}, $options );
        }

        #bring the text matches into the topicMatch hash
        if ($textMatches) {
            @topicMatches{ keys %$textMatches } = values %$textMatches;
        }

        my @scopeTextList = ();
        if ($invertSearch) {
            $topicSet->reset();
            while ( $topicSet->hasNext() ) {
                my $topic = $topicSet->next();

                if ( $topicMatches{$topic} ) {
                }
                else {
                    push( @scopeTextList, $topic );
                }
            }
        }
        else {

            #TODO: the sad thing about this is we lose info
            @scopeTextList = keys(%topicMatches);
        }

        $topicSet =
          new Foswiki::Search::InfoCache( $Foswiki::Plugins::SESSION, $web,
            \@scopeTextList );
    }

    return $topicSet;
}

sub doMongoSearch {
    my $web           = shift;
    my $options    = shift;
    my $scope         = shift;
    my $searchString  = shift;
    my $casesensitive = defined($options->{casesensitive})?$options->{casesensitive}:1;
    
    print STDERR
      "######## Search::MongoDB search ($web) tokens $searchString \n";
    require Foswiki::Plugins::MongoDBPlugin;
    require Foswiki::Plugins::MongoDBPlugin::DB;
    my $collection =
      Foswiki::Plugins::MongoDBPlugin::getMongoDB()->_getCollection('current');
      
    my $mongoQuery = Tie::IxHash->new(
            _web   => $web,
            $scope => { '$regex' => $searchString, '$options' => ($casesensitive? 'i' : '') }
        );

#TODO: Mongo advanced query docco indicates taht /^a/ is faster than /^a.*/ and /^a.*$/ so should refactor to that.
    my $includeTopicsRegex = Foswiki::Search::InfoCache::convertTopicPatternToRegex($options->{topic});
    my $excludeTopicsRegex = Foswiki::Search::InfoCache::convertTopicPatternToRegex( $options->{excludeTopics} );
    
    #Its probably more productive to convert the inc and excl into SearchString bits and pushing it all through the same loops.
    #in fact, what if I conert it all to javascript - that can be used in mapreduce later too, 
    # and then 'guess' which term might give the most performance to extract into the non js query portions.
    if ((($scope eq '_topic')and (($includeTopicsRegex ne '') or ($excludeTopicsRegex ne ''))) or
        (($includeTopicsRegex ne '') and ($excludeTopicsRegex ne ''))){
            $mongoQuery->Push('$where' => 'function() { 
                                            '.convertQueryToJavascript('excludetopics','_topic', $excludeTopicsRegex, '', '!' ).'
                                            '.convertQueryToJavascript('includetopics','_topic', $includeTopicsRegex, '', '' ).'
                                            }');
    } else {
        if ($includeTopicsRegex ne '') {
            $mongoQuery->Push( _topic => { '$regex' => "$includeTopicsRegex" } );
        } elsif ($includeTopicsRegex ne '') {
            $mongoQuery->Push( _topic => { '$not' => { '$regex' => "$excludeTopicsRegex" } } );
        }
    }

    my $cursor = $collection->query($mongoQuery);

    print STDERR "found " . $cursor->count . "\n";

    return $cursor;
}

sub convertQueryToJavascript {
    my $name = shift;
    my $scope = shift;
    my $regex = shift;
    my $regexoptions = shift || '';
    my $not = shift || '';
     
    return '' if ($regex eq '');
     
     return <<"HERE";
            { 
                $name = /$regex/$regexoptions ; 
                matched = $name.test(this.$scope);
                if (matched) {
                    return $not(matched); 
                }
             }
HERE
}


1;
__DATA__
#
# Copyright (C) 2008-2009 Foswiki Contributors. All Rights Reserved.
# Foswiki Contributors are listed in the AUTHORS file in the root
# of this distribution. NOTE: Please extend that file, not this notice.
#
# Additional copyrights apply to some or all of the code in this
# file as follows:
#
# Copyright (C) 2007 TWiki Contributors. All Rights Reserved.
# TWiki Contributors are listed in the AUTHORS file in the root
# of this distribution. NOTE: Please extend that file, not this notice.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version. For
# more details read LICENSE in the root of this distribution.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# As per the GPL, removal of this notice is prohibited.
#
