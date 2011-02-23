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

    print STDERR "HoistMongoDB::hoist from: ", $node->stringify(), "\n"
      if MONITOR
          or WATCH;

    return undef unless ref( $node->{op} );

#TODO: use IxHash to keep the hash order - _some_ parts of queries are order sensitive
#    my %mongoQuery = ();
#    my $ixhQuery = tie( %mongoQuery, 'Tie::IxHash' );
#    $ixhQuery->Push( $scope => $elem );
    my $mongoDBQuery;

    $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::_hoist($node);

    die $mongoDBQuery if (ref($mongoDBQuery) eq '');

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
    my $level = shift || '';

    die 'node eq undef' unless defined($node);

    print STDERR "HoistMongoDB::hoist from: ", $node->stringify(), "\n"
      if MONITOR
          or WATCH;


    #forward propogate that we're inside a 'where' - eg lhs[rhs]
    #sadly, also need to treat a dot case : preferences[value=12].Red
    #   which is the same as preferences[value=12 AND name='Red']
    if ( ( ref( $node->{op} ) eq 'Foswiki::Query::OP_where' )
        or defined( $node->{inWhere} ) )
    {
        $node->{params}[0]->{inWhere} = $node->{inWhere}
          if ( defined( $node->{params}[0] )
            and ( ref( $node->{params}[0] ) ne '' ) );
        $node->{params}[1]->{inWhere} = ( $node->{inWhere} || $node )
          if ( defined( $node->{params}[1] )
            and ( ref( $node->{params}[1] ) ne '' ) );
    }
    print STDERR $level."???????" . ref( $node->{op} ) . " ".($node->{inWhere}?'inWhere':'')."\n" if MONITOR or WATCH;


    #name, or constants.
    if ( not ref( $node->{op} ) ) {
        #use Data::Dumper;
        #print STDERR "not an op (".Dumper($node).")\n" if MONITOR;
        return Foswiki::Query::OP_dot::hoistMongoDB( $node->{op}, $node );
    }
    if ( ref( $node->{op} ) eq 'Foswiki::Query::OP_dot' ) {
        #print STDERR "OP_dot (". $node->{op}->{arity}.")\n" if MONITOR;
        if ( ref( $node->{params}[0]->{op} ) eq 'Foswiki::Query::OP_where' ) {
            #print STDERR "erkle ".Dumper($node->{params}[0])."\n";
    print STDERR "pre erkle::hoist from: ", $node->stringify(), "\n"
      if MONITOR
          or WATCH;

my $rhs_Id = $node->{params}[1]->{params}[0];
if (    #TODO: this is why you can't do this - if the post . portion is an attr name, its not a where selector
    ($rhs_Id ne 'name') and
    ($rhs_Id ne 'val')
                    ) {
            #this is some pretty horrid mucking with reality
            #the rhs of this OP_dot needs to go inside the OP_Where stuff
            #as a name='$rhsval'
            $node->{op}->{arity}--;
            
            
            #TODO: Note - you can't do this - as it won't work if the $name is a registered attr
            my $eq = new Foswiki::Query::OP_eq();
            my $name_node = Foswiki::Query::Node->newLeaf( 'name', Foswiki::Infix::Node::NAME );
            my $eq_node = Foswiki::Query::Node->newNode( $eq, ($name_node, $node->{params}[1]) );
            my $and = new Foswiki::Query::OP_and();
            my @params = $node->{params}[0]->{params}[1];
            push(@params, $eq_node);
            my $and_node = Foswiki::Query::Node->newNode( $and, $node->{params}[0]->{params}[1], $eq_node );

            
            $node->{params}[0]->{params} = [$node->{params}[0]->{params}[0], $and_node];

           
    print STDERR "POST erkle::hoist from: ", $node->stringify(), "\n"
      if MONITOR
          or WATCH;
}

            my $query = _hoist( $node->{params}[0] , $level.' ');
            return $query;
        } else {
            #TODO: really should test for 'simple case' and barf elsewise
            return Foswiki::Query::OP_dot::hoistMongoDB( $node->{op}, $node );
        }
    }

    my $containsQueryFunctions = 0;

    #TODO: if 2 constants(NUMBER,STRING) ASSERT
    #TODO: if the first is a constant, swap
    if ( $node->{op}->{arity} > 0 ) {
        print STDERR "arity 1 \n" if MONITOR;
        $node->{lhs} = _hoist( $node->{params}[0] , $level.' ');
        if ( ref( $node->{lhs} ) ne '' ) {

            print STDERR "ref($node->{lhs}) == ".ref($node->{lhs})."\n" if MONITOR;
            $node->{ERROR} = $node->{lhs}->{ERROR}
              if ( defined( $node->{lhs}->{ERROR} ) );
            $containsQueryFunctions |=
              defined( $node->{lhs}->{'####need_function'} );
            $node->{'####delay_function'} = 1
              if ( defined( $node->{lhs}->{'####delay_function'} ) );
        }
    }

    if (( $node->{op}->{arity} > 1 ) and (defined($node->{params}[1]))) {
        print STDERR "arity 2 \n" if MONITOR;
        $node->{rhs} = _hoist( $node->{params}[1] , $level.' ');
        if ( ref( $node->{rhs} ) ne '' ) {
            $node->{ERROR} = $node->{rhs}->{ERROR}
              if ( defined( $node->{rhs}->{ERROR} ) );
            $containsQueryFunctions |=
              defined( $node->{rhs}->{'####need_function'} );
            $node->{'####delay_function'} = 1
              if ( defined( $node->{rhs}->{'####delay_function'} ) );
        }
    }

    #monitor($node) if MONITOR;

    if ( defined( $node->{'####delay_function'} ) ) {

  #if we're maths, or a brace, return $node, and go for a further delay_function
        if (   ( ref( $node->{op} ) eq 'Foswiki::Query::OP_ob' )
            or ( ref( $node->{op} ) eq 'Foswiki::Query::OP_minus' )
            or ( ref( $node->{op} ) eq 'Foswiki::Query::OP_plus' )
            or ( ref( $node->{op} ) eq 'Foswiki::Query::OP_times' )
            or ( ref( $node->{op} ) eq 'Foswiki::Query::OP_div' )
            or ( ref( $node->{op} ) eq 'Foswiki::Query::OP_pos' )
            or ( ref( $node->{op} ) eq 'Foswiki::Query::OP_neg' ) )
        {

            #print STDERR "POPOPOPOPOPOPOPOPOPOPOPOPOPOPOPOPOPOPOPOPOPOPOP\n";
            return $node->{op}->hoistMongoDB($node);

        }
        else {

            #else, set $containsQueryFunctions
            $containsQueryFunctions = 1;
        }
    }

#DAMMIT, I presume we have oddly nested eval/try catch so throwing isn't working
#throw Error::Simple( 'failed to Hoist ' . ref( $node->{op} ) . "\n" )
# unless ( $node->{op}->can('hoistMongoDB') );
    unless ( $node->{op}->can('hoistMongoDB') ) {
        $node->{ERROR} = 'can\'t Hoist ' . ref( $node->{op} );
    }
    if ( defined( $node->{ERROR} ) ) {
        print STDERR "HOIST ERROR: " . $node->{ERROR};
        die "HOIST ERROR: " . $node->{ERROR};
        return $node;
    }

    #need to convert to js for lc/uc/length  etc :(
    # '####need_function'
    if ($containsQueryFunctions) {
        $node->{lhs} = convertToJavascript( $node->{lhs} )
          if ( ref( $node->{lhs} ) eq 'HASH' );

        my $hoistedNode = $node->{op}->hoistMongoDB($node);
        if ( ref($hoistedNode) eq '' ) {

            #could be a maths op - in which case, eeek?
            #shite - or maths inside braces
            #die 'here: '.$node->{op};
            return $hoistedNode;
        }
        else {
            return { '$where' => convertToJavascript($hoistedNode) };
        }
    }

    return $node->{op}->hoistMongoDB($node);
}

sub monitor {
    my $node = shift;

    print STDERR "MONITOR Hoist node->op="
      . Dumper( $node->{op} )
      . " ref(node->op)="
      . ref( $node->{op} ) . "\n";

    print STDERR "\nparam0(" . $node->{params}[0]->{op} . "): ",
      Data::Dumper::Dumper( $node->{params}[0] ), "\n";
    if ( $node->{op}->{arity} > 1 ) {
        print STDERR "\nparam1(" . $node->{params}[1]->{op} . "): ",
          Data::Dumper::Dumper( $node->{params}[1] ), "\n";
    }

    #TODO: mmm, do we only have unary and binary ops?

    print STDERR "----lhs: " . Data::Dumper::Dumper( $node->{lhs} );
    if ( $node->{op}->{arity} > 1 ) {
        print STDERR "----rhs: " . Data::Dumper::Dumper( $node->{rhs} );
    }
    print STDERR " \n";

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

my %js_func_map = (
    '#lc'     => '.toLowerCase()',
    '#uc'     => '.toUpperCase()',
    '#length' => '.length',
    '#d2n'    => 'foswiki_d2n',
    '#int'    => 'parseInt',
    '#match'  => 'MATCHBANG',
    '#like'   => 'LIKEBANG',
    '#div'    => '/',
    '#mult'   => '*',
    '#plus'   => '+',
    '#minus'  => '-',
    '#pos'    => '+',
    '#neg'    => '-',
);

sub convertFunction {
    my ( $value, $key ) = @_;
    if ( $key eq '#d2n' ) {
        return $js_func_map{$key} . '(' . convertStringToJS($value) . ')';
    }
    if ( $key eq '#int' ) {
        return $js_func_map{$key} . '(' . convertStringToJS($value) . ')';
    }
    if ( $key eq '#match' ) {
        my $regex        = convertStringToJS($value);
        my $regexoptions = '\'\'';
        return "Regex($regex, $regexoptions).test";    #(this.\$scope);";
    }
    if ( $key eq '#like' ) {
        my $regex        = convertStringToJS($value);
        my $regexoptions = '\'\'';
        return "Regex('^'+$regex+'$', $regexoptions).test";   #(this.\$scope);";
    }
    if (   ( $key eq '#div' )
        or ( $key eq '#mult' )
        or ( $key eq '#plus' )
        or ( $key eq '#minus' )
        or ( $key eq '#pos' )
        or ( $key eq '#neg' ) )
    {

        #die 'asd';
        return
            '('
          . convertStringToJS( $$value[0] ) . ')'
          . $js_func_map{$key} . '('
          . convertStringToJS( $$value[1] ) . ')';
    }
    die "$key and $value is not a string? " if ( ref($value) ne '' );
    die "$key is not in the js_func_map"
      if ( not defined( $js_func_map{$key} ) );
    print STDERR "\t\tconvertfunction($value, $key) => \n" if MONITOR;
    return convertStringToJS($value) . $js_func_map{$key};
}

my $fields = '(' . join( '|', keys(%Foswiki::Meta::VALIDATE) ) . ')';
my $ops    = '(' . join( '|', values(%js_op_map) ) . ')';

sub convertStringToJS {
    my $string = shift;
    print STDERR "  convertStringToJS($string)\n" if MONITOR;

    return $string
      if ( $string =~ /^\(.*\)$/ )
      ;    #if we're doing braces, and they're not quoted leave things be

    return convertToJavascript($string) if ( ref($string) eq 'HASH' );

    return $string if ( $string =~ /^'.*'^/ );

#TODO: i _think_ the line below is ok, its needed to make ::test_hoistLengthLHSString work
    return $string if ( $string =~ /^\'.*/ );

    return $string if ( $string =~ /^this\./ );

    # all registered meta type prefixes use a this. in js
    return 'this.' . $string if ( $string =~ /^$fields/ );
    return $string
      if ( $string =~ /^$ops$/ );    #for ops, we only want the entirety

    return $js_op_map{$string} if ( defined( $js_op_map{$string} ) );

    #TODO: generalise - we should not clobber over js_func_map values
    return $string if ( $string =~ /^foswiki_d2n\(.*/ );
    return $string if ( $string eq '<' );

    return 'this.' . $string if ( $string eq '_web' );
    return 'this.' . $string if ( $string eq '_topic' );
    return 'this.' . $string if ( $string eq '_text' );

#if it looks like a number, lets try treating it like a number, and see what happens
#i _think_ this will result in js doing magic just like perl does, as the main diff seems to be that Perl('1'+'1'=2) and JS('1'+'1'='11')
    return $string if ( $string =~ /^[+-]?\d+(\.\d*)?$/ );

    return '\'' . $string . '\'';
}

#converts a mongodb query hash into a $where clause
#frustratingly, both (field:value) and {field:{$op,value}} and {$op, {nodes}} and {$op, []} are valid, making it a little messy
#and {$op, []} might refer to the key outside it (as in $nin), or not, as in $or
sub convertToJavascript {
    my $node      = shift;
    my $statement = '';

#TODO: for some reason the Dumper call makes the HoistMongoDBsTests::test_hoistLcRHSName test succeed - have to work out what i've broken.
    my $dump = Dumper($node);

    print STDERR "\n..............convertToJavascript " . Dumper($node) . "\n"
      if MONITOR;

    while ( my ( $key, $value ) = each(%$node) ) {
        next if ( $key eq '####need_function' );
        next if ( $key eq '####delay_function' );
        $statement .= ' && ' if ( $statement ne '' );

        #BEWARE: if ref($node->{lhs}) eq 'HASH' then $key is going to be wrong.
        if ( ref( $node->{lhs} ) eq 'HASH' ) {
            die 'unexpectedly';
            $key = convertToJavascript( $node->{lhs} );
        }

        #convert $ops into js ones
        my $js_key = $key;

        $js_key = $js_op_map{$key} if ( defined( $js_op_map{$key} ) );
        print STDERR "key = $key ("
          . ref($js_key)
          . ", $js_key), value = $value\n"
          if MONITOR;

        if ( ref($value) eq 'HASH' ) {
            my ( $k, $v ) = each(%$value);
            if ( ( $k eq '$in' ) or ( $k eq '$nin' ) ) {

#TODO: look up to see if javascript thas an value.in(list) or ARRAY.contains(value)
                $statement .= ' ( ' . join(
                    ' || ',
                    map {
                            convertStringToJS($js_key) . ' == '
                          . convertStringToJS($_)
                      } @$v
                ) . ' ) ';
                $statement =
                  ( ( $key eq '$nin' ) ? '!' : '' ) . " ($statement) ";
            }
            elsif ( $key =~ /^\#/ ) {

                $statement .= convertFunction( $value, $key );
            }
            elsif ( $k eq '#match' ) {

                #this is essentially an operator lookahead
                print STDERR ">>>>>>>>>>>>>>>>>>>> lookahead $key -> $k\n"
                  if MONITOR;
                $statement .=
                    convertToJavascript($value) . '('
                  . convertStringToJS($js_key) . ')';
            }
            elsif (( $k eq '#like' )
                or ( $k eq '#div' )
                or ( $k eq '#mult' )
                or ( $k eq '#plus' )
                or ( $k eq '#minus' )
                or ( $k eq '#pos' )
                or ( $k eq '#neg' ) )
            {

                #this is essentially an operator lookahead
                print STDERR ">>>>>>>>>>>>>>>>>>>> lookahead $key -> $k\n"
                  if MONITOR;
                $statement .= ($js_key) . ' ' . convertStringToJS($value);

#$statement .= convertFunction( convertToJavascript($value), $key );
#                $statement .=
#                  convertToJavascript($value).'('.convertStringToJS($js_key).')';
            }
            elsif ( ( $k =~ /^\$/ ) ) {

                #this is essentially an operator lookahead
                $statement .=
                    convertStringToJS($js_key) . ' '
                  . convertToJavascript($value);
            }
            elsif ( $key =~ /^\$/ and defined( $js_op_map{$key} ) ) {
                $statement .= ($js_key) . ' ' . convertStringToJS($value);
            }
            else {
                $statement .=
                    convertStringToJS($js_key) . ' == '
                  . convertToJavascript($value);
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
                    ' ' . convertStringToJS($js_key) . ' ',
                    map { convertToJavascript($_) } @$value
                  ) . ' ) ';

                #$statement = " ($statement) ";
            }
            elsif ( $key =~ /^#/ ) {

                #maths ops
                $statement .= convertFunction( $value, $key );
            }
            else {
                die 'sadly ' . $key;
            }

        }
        else {
            if ( $key eq '$where' ) {
                $statement .= " ($value) ";
            }
            elsif ( $key eq '#where' ) {
                $statement .= " ($value) ";
            }
            else {

                #value isa string..
                #TODO: argh, string or number, er, or regex?
                print STDERR "convertToJavascript - $key => $value is a "
                  . ref($value) . "\n"
                  if MONITOR;
                if ( ref($value) eq 'Regexp' ) {
                    $value =~ /\(\?-xism:(.*)\)/;    #TODO: er, regex options?
                    $statement .=
                      "( /$1/.test(" . convertStringToJS($js_key) . ") )";

                }
                elsif ( $key =~ /^\#/ ) {
                    print STDERR ">>>>>>>>>>>>>>>>>>>> #hash - $key \n"
                      if MONITOR;

                    $statement .= convertFunction( $value, $key );
                }
                elsif ( $key =~ /^\$/ and defined( $js_op_map{$key} ) ) {
                    $statement .= ($js_key) . ' ' . convertStringToJS($value);
                }
                else {

#TODO: can't presume that the 'value' is a constant - it might be a META value name
                    $statement .=
                        convertStringToJS($js_key) . ' == '
                      . convertStringToJS($value);
                }
            }
        }
    }
    print STDERR "----returning $statement\n" if MONITOR;
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
    #use Data::Dumper;
    #print STDERR
    #  "----------------------------------------------------- STUPENDIFY!! - ("
    #  . Dumper($mongoInQuery) . ")\n"
    #  if MONITOR;
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

package Foswiki::Query::OP_like;

#hoist ~ into a mongoDB ixHash query

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    unless ( ref( $node->{rhs} ) eq '' ) {

        #            die 'er, no, can\'t regex on a function';
        # re-write one as $where
        return {
            '$where' =>
              Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::convertToJavascript(
                { $node->{lhs} => { '#like' => $node->{rhs} } }
              )
        };
    }

    my $rhs = quotemeta( $node->{rhs} );
    $rhs =~ s/\\\?/./g;
    $rhs =~ s/\\\*/.*/g;
    $rhs = qr/^$rhs$/;

    return { $node->{lhs} => $rhs };
}

package Foswiki::Query::OP_match;

#hoist =~ into a mongoDB ixHash query

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    unless ( ref( $node->{rhs} ) eq '' ) {

        #            die 'er, no, can\'t regex on a function';
        # re-write one as $where
        return {
            '$where' =>
              Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::convertToJavascript(
                { $node->{lhs} => { '#match' => $node->{rhs} } }
              )
        };
    }

    my $rhs = quotemeta( $node->{rhs} );
    $rhs =~ s/\\\././g;
    $rhs =~ s/\\\*/*/g;

    #marginal speedup, but still every straw
    return {} if ( $rhs eq '.*' );

    $rhs = qr/$rhs/;

    return { $node->{lhs} => $rhs };
}

=begin TML

---++ ObjectMethod Foswiki::Query::OP_dot::hoistMongoDB($node) -> $ref to IxHash


=cut

package Foswiki::Query::OP_dot;
use Foswiki::Meta;

#mongo specific aliases
our %aliases = (
    name => '_topic',
    web  => '_web',
    text => '_text'
);

sub mapAlias {
    my $name = shift;

    #TODO: map to the MongoDB field names (name, web, text, fieldname)
    if ( defined( $aliases{$name} ) ) {
        $name = $aliases{$name};
    }
    elsif ( defined( $Foswiki::Query::Node::aliases{$name} ) ) {
        $name = $Foswiki::Query::Node::aliases{$name};
        $name =~ s/^META://;    #might remove this fomr the mongodb schema
    }
    return $name;
}

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    if ( not defined( $node->{op} ) ) {
        print STDERR 'CONFUSED: ' . Data::Dumper::Dumper($node) . "\n";
        die 'here';

        #return;
    }

#TODO: both the next 2 should only work form registered META - including what params are allowed. (except we don't do that for FIELDS :(
    if ( ref( $node->{op} ) ) {

        #an actual OP_dot
        my $lhs = $node->{params}[0]->{params}[0];
        my $rhs = $node->{params}[1]->{params}[0];

        my $mappedName = mapAlias($lhs);

        #print STDERR "-------------------------------- hoist OP_dot("
        #  . ref( $node->{op} ) . ", "
        #  . Data::Dumper::Dumper($node)
        #  . ")\n  INTO "
        #  . $mappedName . '.'
        #  . $rhs . "\n";

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
        #print STDERR "============================= hoist OP_dot("
        #  . $node->{op} . ", "
        #  . $node->{params}[0] . ', '
        #  . (defined($node->{inWhere})?'inwhere':'notinwhere'). ")\n";

      #if we're in a 'where' eg preferences[name = 'Summary'] then don't aliases
        return $node->{params}[0] if ( defined( $node->{inWhere} ) );

        #if its a registered META, just return it.
        if ( $node->{params}[0] =~ /META:(.*)/ ) {
            return $1 if ( defined( $Foswiki::Meta::VALIDATE{$1} ) );
        }

        my $mappedName = mapAlias( $node->{params}[0] );
        if ( $mappedName ne $node->{params}[0] ) {
            $mappedName =~ s/^META://;
            return $mappedName;
        }
        else {

            #no idea - so we treat it like a field
            return 'FIELD.' . $node->{params}[0] . '.value';
        }
    }
    elsif ( $node->{op} == Foswiki::Infix::Node::NUMBER ) {

        #TODO: would love to convert to numbers, don't think i can yet.
        return $node->{params}[0];
    }
    elsif ( $node->{op} == Foswiki::Infix::Node::STRING ) {
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
                        die 'I have no solution for this query';
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

                   #i think it should be ok, but i've not seen it happen, so die
                    die '---not here yet ' . $key if ( $key ne '$where' );
                }
            }
            else {

                #mmmm, SomeField == '1234' AND SomeField == '2345'?
                #work out if its true or false, and work out something

         #TODO: how about minimising 2 identical non-simple queries ANDed?
         #this eq test below doesn't do identical regex, nor identical $ne etc..
#                    use Data::Dumper;
#                         print STDERR "----+++++++++++++++++ $key ||"
#                           . Dumper($andHash{$key}) . "||"
#                           . Dumper($node->{rhs}->{$key}) . "||"
#                           . ref( $andHash{$key} ) . "||"
#                           . ref( $node->{rhs}->{$key} ) . "||\n";

                if (    ( ref( $andHash{$key} ) eq '' )
                    and ( ref( $node->{rhs}->{$key} ) eq '' ) )
                {
                    #simplest case - both are implicit 'eq'
                    
                    if ( $andHash{$key} eq $node->{rhs}->{$key} ) {
                        #they're the same, ignore the second..
                        $conflictResolved = 1;

                    } else {
#this is able to presume there is no $in, as andHas{key} isa scalar
                        #replace with a $in
                         $andHash{$key} = {
                            '$in' => [
                                $andHash{$key},
                                $node->{rhs}->{$key}
                            ]
                        };
                        $conflictResolved = 1;
                    }
                }
                elsif ( ( ref( $andHash{$key} ) eq 'HASH' ) #if we're already in a non-trivial compare.
                    and ( ref( $node->{rhs}->{$key} ) eq 'HASH' ) )
                {
                    if (    ( defined( $andHash{$key}->{'$ne'} ) )
                        and ( defined( ( $node->{rhs}->{$key}->{'$ne'} ) ) ) )
                    {
#TODO: ERROR: this presumes that there isn't already a $nin. 
                        ###(A != 'qe') AND (A != 'zx') transforms to {A: {$nin: ['qe', 'zx']}} (and regex $ne too?)
                        if (not defined($andHash{$key}->{'$nin'})) {
                            $andHash{$key}->{'$nin'} = ();
                        }
                        push(@{$andHash{$key}->{'$nin'}}, $andHash{$key}->{'$ne'});
                        delete $andHash{$key}->{'$ne'};
                        push(@{$andHash{$key}->{'$nin'}}, $node->{rhs}->{$key}->{'$ne'});

                        $conflictResolved = 1;
                    }
                    else {
                        print STDERR 'doood - not here yet ';
die 'bonus';
                    }
                }
                elsif ( ( ref( $andHash{$key} ) eq 'HASH' )and ( ref( $node->{rhs}->{$key} ) eq '' )) {
                    #$andHash{$key} complex - beware.
                    #$node->{rhs}->{$key} simple - can we toss at $in
                    if (not defined($andHash{$key}->{'$in'})) {
                        $andHash{$key}->{'$in'} = ();
                    }
                    push(@{$andHash{$key}->{'$in'}}, $node->{rhs}->{$key});
                    $conflictResolved = 1;
                } elsif ( ( ref( $andHash{$key} ) eq '' )and ( ref( $node->{rhs}->{$key} ) eq 'HASH' )) {
                         $andHash{$key} = {
                            '$in' => [
                                $andHash{$key}
                            ]
                        };
                        if (defined( $andHash{$key}->{'$ne'} )) {
                            $andHash{$key}->{'$nin'} = [$andHash{$key}->{'$ne'}];
                            $conflictResolved = 1;
                        }
                } else {
                    #db.current.find({_web: 'Sandbox',  '_topic' : {$in : [/Web.*/], $nin : [/.*e$/]}}, {_topic:1})
{
                    print STDERR 'what are we ?';
die 'bonus2';
}
                }
            }
            if ( not $conflictResolved ) {

                if ( defined( $andHash{'$where'} ) ) {

                    # re-write one as $where
                    $andHash{'$where'} =
                      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::convertToJavascript(
                        {
                            $key     => $node->{rhs}->{$key},
                            '#where' => $andHash{'$where'}
                        }
                      );
                }
                else {

                    # re-write one as $where
                    $andHash{'$where'} =
                      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::convertToJavascript(
                        { $key => $node->{rhs}->{$key} } );
                }

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
use Foswiki::Plugins::MongoDBPlugin::HoistMongoDB;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    #no, $not is a dirty little thing that needs to go inside lhs :(
    my $lhs = $node->{lhs};
    die 'not sure' if (ref($lhs) ne 'HASH');    #i don't think I should get here.
    my %query;
    foreach my $key (keys(%$lhs)) {
        #TODO: convertion $nin to $in, and vs versa
        if ($key eq '$where') {
            $query{$key} = '!'. $lhs->{$key};
        } else {
            $query{$key} = {
                            '$not' => $lhs->{$key}
                        };
        }
    }
    return \%query;
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

if (ref($node->{lhs}) ne '') {
    #some darned bugger thought field[name="white"][value="black"] was worth parsing to
    
    #add $node->{rhs} to the lhs' $elemMatch
    my @k = keys(%{$node->{lhs}});
    my @v = each(%{$node->{rhs}});

    $node->{lhs}->{$k[0]}->{'$elemMatch'}->{$v[0]} = $v[1];

    return $node->{lhs};
}

    return {
        $node->{lhs} . '.__RAW_ARRAY' => { '$elemMatch' => $node->{rhs} } };
}

package Foswiki::Query::OP_d2n;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    return { '#d2n' => $node->{lhs}, '####need_function' => 1 };
}

package Foswiki::Query::OP_lc;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    return { '#lc' => $node->{lhs}, '####need_function' => 1 };
}

package Foswiki::Query::OP_length;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    return { '#length' => $node->{lhs}, '####need_function' => 1 };
}

package Foswiki::Query::OP_uc;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    return { '#uc' => $node->{lhs}, '####need_function' => 1 };
}

package Foswiki::Query::OP_int;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    return { '#int' => $node->{lhs}, '####need_function' => 1 };
}

#maths
package Foswiki::Query::OP_div;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    return {
        '#div'               => [ $node->{lhs}, $node->{rhs} ],
        '####delay_function' => 1
    };
}

package Foswiki::Query::OP_minus;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    return {
        '#minus'             => [ $node->{lhs}, $node->{rhs} ],
        '####delay_function' => 1
    };
}

package Foswiki::Query::OP_plus;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    return {
        '#plus'              => [ $node->{lhs}, $node->{rhs} ],
        '####delay_function' => 1
    };
}

package Foswiki::Query::OP_times;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    return {
        '#mult'              => [ $node->{lhs}, $node->{rhs} ],
        '####delay_function' => 1
    };
}

package Foswiki::Query::OP_neg;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    return {
        '#neg'               => [ $node->{lhs}, $node->{rhs} ],
        '####delay_function' => 1,
        '####numeric'        => 1
    };
}

package Foswiki::Query::OP_pos;

sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    return {
        '#pos'               => [ $node->{lhs}, $node->{rhs} ],
        '####delay_function' => 1,
        '####numeric'        => 1
    };
}

package Foswiki::Query::OP_empty;

#TODO: not sure this is the right answer..
sub hoistMongoDB {
    my $op   = shift;
    my $node = shift;

    return {};
}

#array ops from versions.. (m3 work)
package Foswiki::Query::OP_comma;

package Foswiki::Query::OP_in;

######################################
package Foswiki::Query::OP_ref;

#oh what a disaster.
# this has to be implemented as a compound query, so that we're querying against constants
######################################

=pod

mmm, cannot $not a $where

> db.current.find({_web: 'Sandbox',  '_topic' : 'AjaxComment'}, {_topic:1})    
{ "_id" : ObjectId("4d54110940f61a09b1a33186"), "_topic" : "AjaxComment" }
> 
> 
> db.current.find({_web: 'Sandbox',  $where: "this._topic == 'AjaxComment'"}, {_topic:1})
{ "_id" : ObjectId("4d54110940f61a09b1a33186"), "_topic" : "AjaxComment" }
> 
> db.current.find({_web: 'Sandbox',  $where: {$not : "this._topic == 'AjaxComment'"}}, {_topic:1})
error: { "$err" : "invalid use of $not", "code" : 13031 }
> 
> db.current.find({_web: 'Sandbox',  $not: {$where : "this._topic == 'AjaxComment'"}}, {_topic:1})  
error: { "$err" : "invalid operator: $where", "code" : 10068 }

-------------
mmm, can't $not a simple eq (have to unwind to work out to use $ne

> db.current.find({_web: 'Sandbox',  '_topic' : 'AjaxComment'}, {_topic:1})                       
{ "_id" : ObjectId("4d54110940f61a09b1a33186"), "_topic" : "AjaxComment" }
> db.current.find({_web: 'Sandbox',  '_topic' : {'$not':'AjaxComment'}}, {_topic:1})
error: { "$err" : "invalid use of $not", "code" : 13041 }

-------------------
but we can do very fun things with $in and $nin - including regexs

=cut

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
