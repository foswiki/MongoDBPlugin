# See bottom of file for copyright and license details

=begin TML

---+ package Foswiki::Plugins::MongoDBPlugin::HoistMongoDB

extract MonogDB queries from Query Nodes to accellerate querying

=cut

package Foswiki::Plugins::MongoDBPlugin::HoistMongoDB;

use strict;
use warnings;

use Foswiki::Infix::Node ();
use Foswiki::Query::Node ();
use Tie::IxHash          ();
use Data::Dumper;
use Error::Simple;
use Assert;

use Foswiki::Query::HoistREs ();

use constant MONITOR => 0;
use constant WATCH   => 0;

=begin TML

---++ ObjectMethod hoist($query) -> $ref to IxHash


=cut

sub hoist {
    my ( $node, $indent ) = @_;

    return undef unless ref( $node->{op} );

    print STDERR "hoist from: ", $node->stringify(), "\n" if MONITOR or WATCH;

#TODO: use IxHash to keep the hash order - _some_ parts of queries are order sensitive
#    my %mongoQuery = ();
#    my $ixhQuery = tie( %mongoQuery, 'Tie::IxHash' );
#    $ixhQuery->Push( $scope => $elem );
    my $mongoDBQuery;

    $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::_hoist($node);

#TODO: sadly, the exception throwing wasn't working so I'm using a brutish propogate error
    if ( defined( $mongoDBQuery->{ERROR} ) ) {
        print STDERR "AAAAARGH " . $mongoDBQuery->{ERROR} . "\n";
        return;
    }

    print STDERR "Hoisted to:  ",    #$node->stringify(), " -> /",
      Dumper($mongoDBQuery), "/\n"
      if MONITOR or WATCH;

    if ( defined( $Foswiki::cfg{Plugins}{MongoDBPlugin}{UseJavascriptQuery} )
        and $Foswiki::cfg{Plugins}{MongoDBPlugin}{UseJavascriptQuery} )
    {

        #convert the entire query to a javascript $where clause for testing
        return { '$where' => convertToJavascript($mongoDBQuery) };
    }
    else {

#need to test to see if we need to re-write parts of the query in javascript
#ie, nested OR's
#one reason for doing it here, after we've made the mongo queries, is that later, they may implement it and we can remove the kludge
        kludge($mongoDBQuery);
    }

    return $mongoDBQuery;
}

sub kludge {
    my $node = shift;
    my $inOr = shift;

    foreach my $key ( keys(%$node) ) {
        my $value = $node->{$key};
        my $thisIsOr = ( $key eq '$or' );
        if ( $inOr and $thisIsOr ) {

            #nested OR detected, replace with $where and hope
            #try converting to $in first.
            use Foswiki::Query::OP_and;
            my ( $lfield, $lhsIn, $lNum ) = convertOrToIn($value);
            if ( ( $lNum > 0 ) and !defined( $node->{$lfield} ) ) {
                $node->{$lfield} = $lhsIn;
            }
            else {
                $node->{'$where'} = convertToJavascript( { $key => $value } );
            }
            delete $node->{$key};
        }
        else {
            if ( ref($value) eq 'HASH' ) {
                kludge( $value, ( $inOr or $thisIsOr ) );
            }
            elsif ( ref($value) eq 'ARRAY' ) {
                next if ( $key eq '$in' );
                next if ( $key eq '$nin' );
                foreach my $n (@$value) {
                    kludge( $n, ( $inOr or $thisIsOr ) );
                }
            }
            else {
            }
        }
    }
}

sub _hoist {
    my $node = shift;

    die 'node eq undef' unless defined($node);

    print STDERR "???????" . ref( $node->{op} ) . "\n" if MONITOR or WATCH;
    
    #forward propogate that we're inside a 'where' - eg lhs[rhs]
    if (( ref( $node->{op} ) eq 'Foswiki::Query::OP_where' )
        or defined( $node->{inWhere} ) 
        )
    {
print STDERR "[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]";
        $node->{params}[0]->{inWhere} = $node->{inWhere}
          if ( defined( $node->{params}[0] ) and (ref($node->{params}[0]) ne ''));
        $node->{params}[1]->{inWhere} = ( $node->{inWhere} || $node )
          if ( defined( $node->{params}[1] )  and (ref($node->{params}[1]) ne '') );
    }

    #name, or constants.
    if ( !ref( $node->{op} ) ) {
        return Foswiki::Query::OP_dot::hoistMongoDB( $node->{op}, $node );
    }
    if ( ref( $node->{op} ) eq 'Foswiki::Query::OP_dot' ) {
        return Foswiki::Query::OP_dot::hoistMongoDB( $node->{op}, $node );
    }


    #TODO: if 2 constants(NUMBER,STRING) ASSERT
    #TODO: if the first is a constant, swap

    $node->{lhs} = _hoist( $node->{params}[0] )
      if ( $node->{op}->{arity} > 0 );
    $node->{ERROR} = $node->{lhs}->{ERROR}
      if ( ref( $node->{lhs} ) and defined( $node->{lhs}->{ERROR} ) );

    $node->{rhs} = _hoist( $node->{params}[1] )
      if ( $node->{op}->{arity} > 1 );
    $node->{ERROR} = $node->{rhs}->{ERROR}
      if ( ref( $node->{rhs} ) and defined( $node->{rhs}->{ERROR} ) );

    monitor($node);

#DAMMIT, I presume we have oddly nested eval/try catch so throwing isn't working
#throw Error::Simple( 'failed to Hoist ' . ref( $node->{op} ) . "\n" )
# unless ( $node->{op}->can('hoistMongoDB') );
    unless ( $node->{op}->can('hoistMongoDB') ) {
        $node->{ERROR} = 'can\'t Hoist ' . ref( $node->{op} );
    }
    if ( defined( $node->{ERROR} ) ) {
        print STDERR "HOIST ERROR: " . $node->{ERROR};
        return $node;
    }

    return $node->{op}->hoistMongoDB($node);
}

sub monitor {
    my $node = shift;

    print STDERR "\nparam0(" . $node->{params}[0]->{op} . "): ",
      Data::Dumper::Dumper( $node->{params}[0] ), "\n"
      if MONITOR;
    print STDERR "\nparam1(" . $node->{params}[1]->{op} . "): ",
      Data::Dumper::Dumper( $node->{params}[1] ), "\n"
      if MONITOR;

    #TODO: mmm, do we only have unary and binary ops?

    print STDERR "----lhs: "
      . Data::Dumper::Dumper( $node->{lhs} )
      . "----rhs: "
      . Data::Dumper::Dumper( $node->{rhs} ) . " \n"
      if MONITOR;

#print STDERR "HoistS ",$query->stringify()," -> /",Dumper($mongoDBQuery),"/\n";

    print STDERR "Hoist node->op="
      . Dumper( $node->{op} )
      . " ref(node->op)="
      . ref( $node->{op} ) . "\n"
      if MONITOR;
}

#map mongodb $ops to javascript logic
my %js_op_map = (
    '$eq'  => '==',    #doesn't exist, probly should for simplicity
    '$ne'  => '!=',
    '$not' => '!',
    '$lt'  => '<',
    '$lte' => '<=',
    '$gt'  => '>',
    '$gte' => '>=',
    '$and' => '&&',
    '$or'  => '||',

    #'' => '',
);

#converts a mongodb query hash into a $where clause
#frustratingly, both (field:value) and {field:{$op,value}} and {$op, {nodes}} and {$op, []} are valid, making it a little messy
#and {$op, []} might refer to the key outside it (as in $nin), or not, as in $or
sub convertToJavascript {
    my $node      = shift;
    my $statement = '';

    #print STDERR "\n..............DUMPER: ".Dumper($node)."\n";

    while ( my ( $key, $value ) = each(%$node) ) {
        $statement .= ' && ' if ( $statement ne '' );

        #convert $ops into js ones
        my $js_key = $key;
        $js_key = $js_op_map{$key} if ( defined( $js_op_map{$key} ) );

        if ( ref($value) eq 'HASH' ) {
            my ( $k, $v ) = each(%$value);
            if ( ( $k eq '$in' ) or ( $k eq '$nin' ) ) {

#TODO: look up to see if javascrip thas an value.in(list) or ARRAY.contains(value)
                $statement .=
                  ' ( ' . join( ' || ', map { "$js_key == $_" } @$v ) . ' ) ';
                $statement =
                  ( ( $key eq '$nin' ) ? '!' : '' ) . " ($statement) ";
            }
            else {
                $value = convertToJavascript($value);

                $statement .= "$js_key $value";
            }
        }
        elsif ( ref($value) eq 'ARRAY' ) {
            if ( ( $key eq '$in' ) or ( $key eq '$nin' ) ) {
                die 'unpleasently'
                  ; #should never get here - it needs to be handled while we knoe the field it refers to
            }
            elsif ( $key eq '$or' ) {

                #er, assuming $key == $or - $in and $nin will kick me
                $statement .= ' ( '
                  . join(
                    ' ' . $js_key . ' ',
                    map { convertToJavascript($_) } @$value
                  ) . ' ) ';

                #$statement = " ($statement) ";
            }
            else {
                die 'sadly ' . $key;
            }

        }
        else {
            if ( $key eq '$where' ) {
                $statement = " ($value) ";
            }
            else {

            #value isa string..
            #TODO: argh, string or number, er, or regex?
            #print STDERR "convertToJavascript - $value is a ".ref($value)."\n";
                if ( ref($value) eq 'Regexp' ) {
                    $value =~ /\(\?-xism:(.*)\)/;    #TODO: er, regex options?
                    $statement .= "( /$1/.test(this.$js_key) )";
                }
                else {
                    $statement .= "this.$js_key == '$value'";
                }
            }
        }
    }
    return $statement;
}

#---+++ convertOrToIn($refToOrArray) -> ($field, $mongoInQuery, numberOfElements)
#if we can convert to in / nin, do so, else return undef
#return partial query hash - ie {$in : []} or {$nin : []}
sub convertOrToIn {
    my $orArrayRef = shift;

    my $fieldname;
    my $mongoInQuery;
    my $numberOfElements = 0;

    my $failedToConvertToOnlyIn = 0;

#if more than 2 elements in the ARRAY are the same key, and same negation, aggregate into $in / $nin
#IFF the elements in the array are simple - otherwise, need to kick out
    my $keys = {};
    my @complex;
    foreach my $elem (@$orArrayRef) {
        $numberOfElements++;
        my ( $firstKey, $moreKeys ) = keys(%$elem);
        my $firstVal = $elem->{$firstKey};
        if ( defined($moreKeys)
            or not( ( ref($firstVal) eq '' ) or ( ref($firstVal) eq 'Regexp' ) )
          )
        {
            print STDERR "------ too complex ($firstKey,$firstVal == "
              . ref($firstVal) . ")\n"
              if MONITOR;

            #actually, if its an $in/$nin we could do something..
            push( @complex, $elem );
            $failedToConvertToOnlyIn = 1;
        }
        else {

            #just do $in for now
            print STDERR "------ simple ($firstKey, $firstVal)\n" if MONITOR;
            push( @{ $keys->{$firstKey} }, $firstVal );
        }
    }

    while ( my ( $k, $v ) = each(%$keys) ) {
        my @array = @$v;

        print STDERR "------ erg $#array\n" if MONITOR;

        if ( $#array >= 1 ) {
            print STDERR "------ $k => \$in @$v\n" if MONITOR;

            push( @complex, { $k => { '$in' => $v } } );
        }
        else {
            print STDERR "------ $k => $v->[0]\n" if MONITOR;
            push( @complex, { $k => $v->[0] } );
        }
    }

    if ( scalar(@complex) > 1 ) {
        $failedToConvertToOnlyIn = 1;

        #$optimisedOr->{'$or'} = \@complex;
    }
    else {

        #success - its an $in?
        ( $fieldname, $mongoInQuery ) = each( %{ $complex[0] } );
    }
    use Data::Dumper;
    print STDERR
      "----------------------------------------------------- STUPENDIFY!! - ("
      . Dumper($mongoInQuery) . ")\n"
      if MONITOR;
    return ( $fieldname, $mongoInQuery,
        ( $failedToConvertToOnlyIn ? 0 : $numberOfElements ) );
}

########################################################################################
#Hoist the OP's

=begin TML

---++ ObjectMethod Foswiki::Query::OP_eq::hoistMongoDB($node) -> $ref to IxHash

hoist ~ into a mongoDB ixHash query

=cut

package Foswiki::Query::OP_eq;
use Assert;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    ASSERT( $node->{op}->{name} eq '=' ) if DEBUG;
    return { $node->{lhs} => $node->{rhs} };
}

=begin TML

---++ ObjectMethod Foswiki::Query::OP_like::hoistMongoDB($node) -> $ref to IxHash

hoist ~ into a mongoDB ixHash query

=cut

package Foswiki::Query::OP_like;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    my $rhs = quotemeta( $node->{rhs} );
    $rhs =~ s/\\\?/./g;
    $rhs =~ s/\\\*/.*/g;
    $rhs = qr/$rhs/;

    return { $node->{lhs} => $rhs };
}

=begin TML

---++ ObjectMethod Foswiki::Query::OP_dot::hoistMongoDB($node) -> $ref to IxHash

hoist ~ into a mongoDB ixHash query

=cut

package Foswiki::Query::OP_dot;
our %aliases = (

    #    attachments => 'META:FILEATTACHMENT',
    #    fields      => 'META:FIELD',
    #    form        => 'META:FORM',
    #    info        => 'META:TOPICINFO',
    #    moved       => 'META:TOPICMOVED',
    #    parent      => 'META:TOPICPARENT',
    #    preferences => 'META:PREFERENCE',
    name => '_topic',
    web  => '_web',
    text => '_text'
);

sub mapAlias {
    my $name = shift;

    #TODO: map to the MongoDB field names (name, web, text, fieldname)
      if ( defined( $aliases{ $name  }) ) {
        $name = $aliases{ $name };
      } elsif ( defined( $Foswiki::Query::Node::aliases{$name} ) ) {
        $name = $Foswiki::Query::Node::aliases{$name};
        $name =~ s/^META://;    #might remove this fomr the mongodb schema
    }
    return $name;
}

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    if ( !defined( $node->{op} ) ) {
        print STDERR 'CONFUSED: ' . Data::Dumper::Dumper($node) . "\n";
        die 'here';

        #return;
    }

    if ( ref( $node->{op} ) ) {

        #an actual OP_dot
        my $lhs = $node->{params}[0];
        my $rhs = $node->{params}[1];

        #        ASSERT( !ref( $lhs->{op} ) ) if DEBUG;
        #        ASSERT( !ref( $rhs->{op} ) ) if DEBUG;
        #        ASSERT( $lhs->{op} eq Foswiki::Infix::Node::NAME ) if DEBUG;
        #        ASSERT( $rhs->{op} eq Foswiki::Infix::Node::NAME )
        #          if DEBUG;

        $lhs = $lhs->{params}[0];
        $rhs = $rhs->{params}[0];
        my $mappedName = mapAlias($lhs);

        print STDERR "-------------------------------- hoist OP_dot("
          . ref( $node->{op} ) . ", "
          . Data::Dumper::Dumper($node)
          . ")\n INTO "
          . $mappedName . '.'
          . $rhs . "\n";

        if ( $mappedName ne $lhs ) {
            return $mappedName . '.' . $rhs;
        }
        else {

            # TODO: assumes the term before the dot is the form name??? gads
            return 'FIELD.' . $rhs . '.value';
        }
    }
    elsif ( $node->{op} == Foswiki::Infix::Node::NAME ) {

        #if we're in a where, this is a bit transmissive
        print STDERR "============================= hoist OP_dot("
          . $node->{op} . ", "
          . $node->{params}[0] . ', '
          . (defined($node->{inWhere})?'inwhere':'notinwhere'). ")\n";
          
        #if we're in a 'where' eg preferences[name = 'Summary'] then don't aliases
        return $node->{params}[0] if (defined($node->{inWhere}));

        my $mappedName = mapAlias($node->{params}[0]);
        if ($mappedName ne $node->{params}[0]) {
            $mappedName =~ s/^META://;
            return $mappedName;
        } else {
            #no idea - so we treat it like a field
            return 'FIELD.' . $node->{params}[0] . '.value';
        }
    }
    elsif (( $node->{op} == Foswiki::Infix::Node::NUMBER )
        or ( $node->{op} == Foswiki::Infix::Node::STRING ) )
    {
        return $node->{params}[0];
    }
}

package Foswiki::Query::OP_and;
use Foswiki::Plugins::MongoDBPlugin::HoistMongoDB;
use Assert;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

#beware, can't have the same key in both lhs and rhs, as the hash collapses them into one
#this is more a limitation of the mongodb drivers - internally, mongodb (i'm told) can doit.

    my %andHash = %{ $node->{lhs} };
    foreach my $key ( keys( %{ $node->{rhs} } ) ) {
        if ( defined( $andHash{$key} ) ) {
            my $conflictResolved = 0;
            if ( $key =~ /^\$.*/ ) {

                #its an operator $or, $in, $nin,
                if ( $key eq '$or' ) {

        #if one of the OR's happens to be all on the same field, then its simple
        #if not, (A OR B) AND (C OR D) == (A OR B) AND NOT (NOT C AND NOT D) :(
        #EXCEPT of course, that mongo doesn't have NOT. - but... we do have $nor
        #A AND B == NOT (NOT A OR NOT B) == $nor: {A $ne banana, B $ne trifle}

                    my ( $rfield, $rhsIn, $rNum ) =
                      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::convertOrToIn(
                        $node->{rhs}->{$key} );
                    my ( $lfield, $lhsIn, $lNum ) =
                      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::convertOrToIn(
                        $andHash{$key} );

                    if ( ( $rNum == 0 ) and ( $lNum == 0 ) ) {
                        print STDERR 'I have no solution for this query';
                    }
                    else {
                        if ( ( $rNum > 0 ) and ( $lNum > 0 ) ) {
                            if ( $rfield eq $lfield ) {

                                #bummer, have to choose one or the other
                                if ( $rNum > $lNum ) {
                                    $andHash{$rfield} = $rhsIn;
                                }
                                else {
                                    $andHash{$lfield} = $lhsIn;
                                    $andHash{'$or'} = $node->{rhs}->{$key};
                                }
                            }
                            else {
                                $andHash{$rfield} = $rhsIn;
                                $andHash{$lfield} = $lhsIn;
                                delete $andHash{'$or'};
                            }
                        }
                        elsif ( $rNum > 0 ) {
                            $andHash{$rfield} = $rhsIn;
                        }
                        else {
                            $andHash{$lfield} = $lhsIn;
                            $andHash{'$or'} = $node->{rhs}->{$key};
                        }
                        $conflictResolved = 1;
                    }

                }
                else {
                    print STDERR 'not here yet ' . $key;
                }
            }
            else {

                #mmmm, SomeField == '1234' AND SomeField == '2345'?
                #work out if its true or false, and work out something

         #TODO: how about minimising 2 identical non-simple queries ANDed?
         #this eq test below doesn't do identical regex, nor identical $ne etc..
                print STDERR "----+++++++++++++++++ ||"
                  . $andHash{$key} . "||"
                  . $node->{rhs}->{$key} . "||"
                  . ref( $andHash{$key} ) . "||"
                  . ref( $node->{rhs}->{$key} ) . "||\n";
                if (    ( ref( $andHash{$key} ) eq '' )
                    and ( ref( $node->{rhs}->{$key} ) eq '' )
                    and ( $andHash{$key} eq $node->{rhs}->{$key} ) )
                {

                    #they're the same, ignore the second..
                    $conflictResolved = 1;
                    print STDERR "bump\n";
                }
                elsif ( ( ref( $andHash{$key} ) eq 'HASH' )
                    and ( ref( $node->{rhs}->{$key} ) eq 'HASH' )
                    and ( defined( $andHash{$key}->{'$ne'} ) )
                    and ( defined( ( $node->{rhs}->{$key}->{'$ne'} ) ) ) )
                {
                    ###(A != 'qe') AND (A != 'zx') transforms to {A: {$nin: ['qe', 'zx']}} (and regex $ne too?)
                    $andHash{$key} = {
                        '$nin' => [
                            $andHash{$key}->{'$ne'},
                            $node->{rhs}->{$key}->{'$ne'}
                        ]
                    };
                    $conflictResolved = 1;
                }
                else {
                    use Data::Dumper;
                    print STDERR 'field - not here yet ' 
                      . $key
                      . '   ----   '
                      . Dumper( $andHash{$key} )
                      . '    ----    '
                      . Dumper( $node->{rhs}->{$key} );
                }
            }
            if ( not $conflictResolved ) {

            #i don't think i've implemented convertToJavascript to do $where too
                die 'argh@' if ( defined( $andHash{'$where'} ) );

                print STDERR
                  "MongoDB cannot AND 2 queries with the same key ($key) \n";

                # re-write one as $where
                $andHash{'$where'} =
                  Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::convertToJavascript(
                    { $key => $node->{rhs}->{$key} } );
            }
        }
        else {
            $andHash{$key} = $node->{rhs}->{$key};
        }

    }

    return \%andHash;
}

package Foswiki::Query::OP_or;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    my $mongoQuery;

    my $lhs = $node->{lhs};

    #need to detect nested OR's and unwind them
    if ( defined( $lhs->{'$or'} ) ) {
        $lhs = $lhs->{'$or'};

        #print STDERR "---+++--- $lhs, ".ref($lhs)."\n";
        $mongoQuery = { '$or' => [ @$lhs, $node->{rhs} ] };
    }
    elsif ( defined( $node->{rhs}->{'$or'} ) ) {

        #i'm somewhat sure this can't happen.
        my $rhs = $node->{rhs}->{'$or'};
        die "---+++--- TTHATS A SURPRISE: $rhs, " . ref($rhs) . "\n";
        $mongoQuery = { '$or' => [ $lhs, @$rhs ] };
    }
    else {
        $mongoQuery = { '$or' => [ $lhs, $node->{rhs} ] };
    }

    return $mongoQuery;
}

package Foswiki::Query::OP_not;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    return { '$not' => $node->{lhs} };
}

package Foswiki::Query::OP_gte;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    return { $node->{lhs} => { '$gte' => $node->{rhs} } };
}

package Foswiki::Query::OP_gt;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    return { $node->{lhs} => { '$gt' => $node->{rhs} } };
}

package Foswiki::Query::OP_lte;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    return { $node->{lhs} => { '$lte' => $node->{rhs} } };
}

package Foswiki::Query::OP_lt;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    return { $node->{lhs} => { '$lt' => $node->{rhs} } };
}

package Foswiki::Query::OP_match;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    my $rhs = quotemeta( $node->{rhs} );
    $rhs =~ s/\\\././g;
    $rhs =~ s/\\\*/*/g;

    #marginal speedup, but still every straw
    return {} if ( $rhs eq '.*' );

    $rhs = qr/$rhs/;

    return { $node->{lhs} => $rhs };
}

package Foswiki::Query::OP_ne;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    return { $node->{lhs} => { '$ne' => $node->{rhs} } };
}

package Foswiki::Query::OP_ob;

# ( )
sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    return $node->{lhs};
}

package Foswiki::Query::OP_d2n;

package Foswiki::Query::OP_lc;

package Foswiki::Query::OP_length;

package Foswiki::Query::OP_ref;

package Foswiki::Query::OP_uc;

package Foswiki::Query::OP_where;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

#AHA. this needs to use $elemMatch
#> t.find( { x : { $elemMatch : { a : 1, b : { $gt : 1 } } } } )
#{ "_id" : ObjectId("4b5783300334000000000aa9"),
#"x" : [ { "a" : 1, "b" : 3 }, 7, { "b" : 99 }, { "a" : 11 } ]
#}
#and thus, need to re-do the mongodb schema so that meta 'arrays' are arrays again.
#and that means the FIELD: name based shorcuts need to be re-written :/ de-indexing the queries :(

    print "************************************* giber splat";

    return { $node->{lhs}.'.__RAW_ARRAY' => { '$elemMatch' => $node->{rhs} } };
}

1;
__DATA__

Module of Foswiki - The Free and Open Source Wiki, http://foswiki.org/, http://Foswiki.org/

Copyright (C) 2010 Foswiki Contributors. All Rights Reserved.
Foswiki Contributors are listed in the AUTHORS file in the root
of this distribution. NOTE: Please extend that file, not this notice.

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version. For
more details read LICENSE in the root of this distribution.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

As per the GPL, removal of this notice is prohibited.

Author: Sven Dowideit http://fosiki.com
