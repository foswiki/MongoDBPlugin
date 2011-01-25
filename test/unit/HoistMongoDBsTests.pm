# Test for hoisting REs from query expressions
package HoistMongoDBsTests;

use FoswikiFnTestCase;
our @ISA = qw( FoswikiFnTestCase );

use Foswiki::Query::Parser;
use Foswiki::Plugins::MongoDBPlugin::HoistMongoDB;
use Foswiki::Query::Node;
use Foswiki::Meta;
use Data::Dumper;
use strict;

#list of operators we can output
my @MongoOperators = qw/$or $not $nin $in/;
#list of all Query ops
#TODO: build this from code?
my @QueryOps = qw/== != > < =~ ~/;

#TODO: use the above to test operator coverage - fail until we have full coverage.
#TODO: test must run _last_


sub do_Assert {
    my $this                 = shift;
    my $query                = shift;
    my $mongoDBQuery         = shift;
    my $expectedMongoDBQuery = shift;

    #    print STDERR "HoistS ",$query->stringify();
    print STDERR "HoistS ", Dumper($query);
    print STDERR "\n -> /", Dumper($mongoDBQuery), "/\n";

    $this->assert_deep_equals( $expectedMongoDBQuery, $mongoDBQuery );

   #try out converttoJavascript
   print STDERR "\nconvertToJavascript: \n".Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::convertToJavascript($mongoDBQuery)."\n";
}

sub set_up {
    my $this = shift;
    $this->SUPER::set_up();

    my $meta = Foswiki::Meta->new( $this->{session}, 'Web', 'Topic' );
    $meta->putKeyed(
        'FILEATTACHMENT',
        {
            name    => "att1.dat",
            attr    => "H",
            comment => "Wun",
            path    => 'a path',
            size    => '1',
            user    => 'Junkie',
            rev     => '23',
            date    => '25',
        }
    );
    $meta->putKeyed(
        'FILEATTACHMENT',
        {
            name    => "att2.dot",
            attr    => "",
            comment => "Too",
            path    => 'anuvver path',
            size    => '100',
            user    => 'ProjectContributor',
            rev     => '105',
            date    => '99',
        }
    );
    $meta->put( 'FORM', { name => 'TestForm' } );
    $meta->put(
        'TOPICINFO',
        {
            author  => 'AlbertCamus',
            date    => '12345',
            format  => '1.1',
            version => '1.1913',
        }
    );
    $meta->put(
        'TOPICMOVED',
        {
            by   => 'AlbertCamus',
            date => '54321',
            from => 'BouvardEtPecuchet',
            to   => 'ThePlague',
        }
    );
    $meta->put( 'TOPICPARENT', { name => '' } );
    $meta->putKeyed( 'PREFERENCE', { name => 'Red',    value => '0' } );
    $meta->putKeyed( 'PREFERENCE', { name => 'Green',  value => '1' } );
    $meta->putKeyed( 'PREFERENCE', { name => 'Blue',   value => '0' } );
    $meta->putKeyed( 'PREFERENCE', { name => 'White',  value => '0' } );
    $meta->putKeyed( 'PREFERENCE', { name => 'Yellow', value => '1' } );
    $meta->putKeyed( 'FIELD',
        { name => "number", title => "Number", value => "99" } );
    $meta->putKeyed( 'FIELD',
        { name => "string", title => "String", value => "String" } );
    $meta->putKeyed(
        'FIELD',
        {
            name  => "StringWithChars",
            title => "StringWithChars",
            value => "n\nn t\tt s\\s q'q o#o h#h X~X \\b \\a \\e \\f \\r \\cX"
        }
    );
    $meta->putKeyed( 'FIELD',
        { name => "boolean", title => "Boolean", value => "1" } );
    $meta->putKeyed( 'FIELD', { name => "macro", value => "%RED%" } );

    $meta->{_text} = "Green ideas sleep furiously";

    $this->{meta} = $meta;
}

sub test_hoistSimple {
    my $this        = shift;
    my $s           = "number=99";
    my $queryParser = new Foswiki::Query::Parser();
    my $query       = $queryParser->parse($s);
    my $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);

    $this->do_Assert( $query, $mongoDBQuery, { 'FIELD.number.value' => '99' } );
}

sub test_hoistSimple_OP_Like {
    my $this        = shift;
    my $s           = "String~'.*rin.*'";
    my $queryParser = new Foswiki::Query::Parser();
    my $query       = $queryParser->parse($s);
    my $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);
    $this->do_Assert( $query, $mongoDBQuery,
        { 'FIELD.String.value' => qr/(?-xism:\..*rin\..*)/ } );
}

sub test_hoistSimple2 {
    my $this        = shift;
    my $s           = "99=number";
    my $queryParser = new Foswiki::Query::Parser();
    my $query       = $queryParser->parse($s);
    my $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);

#TODO: should really reverse these, but it is harder with strings - (i think the lhs in  'web.topic'/something is a string..
    $this->do_Assert( $query, $mongoDBQuery, { '99' => 'FIELD.number.value' } );
}

sub test_hoistOR {
    my $this        = shift;
    my $s           = "number=12 or string='bana'";
    my $queryParser = new Foswiki::Query::Parser();
    my $query       = $queryParser->parse($s);
    my $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);

    $this->do_Assert(
        $query,
        $mongoDBQuery,
        {
            '$or' => [
                { 'FIELD.number.value' => '12' },
                { 'FIELD.string.value' => 'bana' }
            ]
        }
    );
}

sub test_hoistOROR {
    my $this        = shift;
    my $s           = "number=12 or string='bana' or string = 'apple'";
    my $queryParser = new Foswiki::Query::Parser();
    my $query       = $queryParser->parse($s);
    my $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);

    $this->do_Assert(
        $query,
        $mongoDBQuery,
        {
            '$or' => [
                { 'FIELD.number.value' => '12' },
                { 'FIELD.string.value' => 'bana' },
                { 'FIELD.string.value' => 'apple' }
            ]
        }
    );
}

sub test_hoistBraceOROR {
    my $this        = shift;
    my $s           = "(number=12 or string='bana' or string = 'apple')";
    my $queryParser = new Foswiki::Query::Parser();
    my $query       = $queryParser->parse($s);
    my $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);

    $this->do_Assert(
        $query,
        $mongoDBQuery,
        {
            '$or' => [
                { 'FIELD.number.value' => '12' },
                { 'FIELD.string.value' => 'bana' },
                { 'FIELD.string.value' => 'apple' }
            ]
        }
    );
}

sub test_hoistANDBraceOROR {
    my $this = shift;
    my $s =
      "(number=12 or string='bana' or string = 'apple') AND (something=12)";
    my $queryParser = new Foswiki::Query::Parser();
    my $query       = $queryParser->parse($s);
    my $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);

    $this->do_Assert(
        $query,
        $mongoDBQuery,
        {
            'FIELD.something.value' => '12',
            '$or'                   => [
                { 'FIELD.number.value' => '12' },
                { 'FIELD.string.value' => 'bana' },
                { 'FIELD.string.value' => 'apple' }
            ]
        }
    );
}


sub test_hoistBraceANDBrace_OPTIMISE {
    my $this = shift;
    my $s    = "(TargetRelease = 'minor') AND (TargetRelease = 'minor')";
    my $queryParser = new Foswiki::Query::Parser();
    my $query       = $queryParser->parse($s);
    my $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);

    $this->do_Assert(
        $query,
        $mongoDBQuery,
        {
            'FIELD.TargetRelease.value' => 'minor' 
        }
    );
}

#need to optimise it, as mongo cna't have 2 keys of the same name, its queries are a hash
sub test_hoistBraceANDBrace {
    my $this = shift;
    my $s    = "(TargetRelease != 'minor') AND (TargetRelease != 'major')";
    my $queryParser = new Foswiki::Query::Parser();
    my $query       = $queryParser->parse($s);
    my $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);

    $this->do_Assert(
        $query,
        $mongoDBQuery,
        {
          'FIELD.TargetRelease.value' => {
                                           '$nin' => [
                                                       'minor',
                                                       'major'
                                                     ]
                                         }
        }
    );
}

sub test_hoistBrace {
    my $this        = shift;
    my $s           = "(number=12)";
    my $queryParser = new Foswiki::Query::Parser();
    my $query       = $queryParser->parse($s);
    my $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);

    $this->do_Assert( $query, $mongoDBQuery, { 'FIELD.number.value' => '12' } );
}

sub test_hoistAND {
    my $this        = shift;
    my $s           = "number=12 and string='bana'";
    my $queryParser = new Foswiki::Query::Parser();
    my $query       = $queryParser->parse($s);
    my $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);

    $this->do_Assert(
        $query,
        $mongoDBQuery,
        {
            'FIELD.number.value' => '12',
            'FIELD.string.value' => 'bana'
        }
    );
}

sub test_hoistANDAND {
    my $this        = shift;
    my $s           = "number=12 and string='bana' and something='nothing'";
    my $queryParser = new Foswiki::Query::Parser();
    my $query       = $queryParser->parse($s);
    my $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);

    $this->do_Assert(
        $query,
        $mongoDBQuery,
        {
            'FIELD.number.value'    => '12',
            'FIELD.something.value' => 'nothing',
            'FIELD.string.value'    => 'bana'
        }
    );
}

sub test_hoistSimpleDOT {
    my $this        = shift;
    my $s           = "number.bana = 12";
    my $queryParser = new Foswiki::Query::Parser();
    my $query       = $queryParser->parse($s);
    my $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);

    #TODO: there's and assumption that the bit before the . is the form-name
    $this->do_Assert( $query, $mongoDBQuery, { 'FIELD.bana.value' => '12' } );
}

sub test_hoistGT {
    my $this        = shift;
    my $s           = "number>12";
    my $queryParser = new Foswiki::Query::Parser();
    my $query       = $queryParser->parse($s);
    my $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);

    $this->do_Assert( $query, $mongoDBQuery,
        { 'FIELD.number.value' => { '$gt' => '12' } } );
}

sub test_hoistGTE {
    my $this        = shift;
    my $s           = "number>=12";
    my $queryParser = new Foswiki::Query::Parser();
    my $query       = $queryParser->parse($s);
    my $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);

    $this->do_Assert( $query, $mongoDBQuery,
        { 'FIELD.number.value' => { '$gte' => '12' } } );
}

sub test_hoistLT {
    my $this        = shift;
    my $s           = "number<12";
    my $queryParser = new Foswiki::Query::Parser();
    my $query       = $queryParser->parse($s);
    my $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);

    $this->do_Assert( $query, $mongoDBQuery,
        { 'FIELD.number.value' => { '$lt' => '12' } } );
}

sub test_hoistLTE {
    my $this        = shift;
    my $s           = "number<=12";
    my $queryParser = new Foswiki::Query::Parser();
    my $query       = $queryParser->parse($s);
    my $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);

    $this->do_Assert( $query, $mongoDBQuery,
        { 'FIELD.number.value' => { '$lte' => '12' } } );
}

sub test_hoistEQ {
    my $this        = shift;
    my $s           = "number=12";
    my $queryParser = new Foswiki::Query::Parser();
    my $query       = $queryParser->parse($s);
    my $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);

    $this->do_Assert( $query, $mongoDBQuery, { 'FIELD.number.value' => '12' } );
}

sub test_hoistNE {
    my $this        = shift;
    my $s           = "number!=12";
    my $queryParser = new Foswiki::Query::Parser();
    my $query       = $queryParser->parse($s);
    my $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);

    $this->do_Assert( $query, $mongoDBQuery,
        { 'FIELD.number.value' => { '$ne' => '12' } } );
}

sub test_hoistNOT_EQ {
    my $this        = shift;
    my $s           = "not(number=12)";
    my $queryParser = new Foswiki::Query::Parser();
    my $query       = $queryParser->parse($s);
    my $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);
    $this->do_Assert( $query, $mongoDBQuery,
        { '$not' => { 'FIELD.number.value' => '12' } } );
}

sub test_hoistCompound {
    my $this = shift;
    my $s =
"number=99 AND string='String' and (moved.by='AlbertCamus' OR moved.by ~ '*bert*')";
    my $queryParser = new Foswiki::Query::Parser();
    my $query       = $queryParser->parse($s);
    my $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);
    $this->do_Assert(
        $query,
        $mongoDBQuery,
        {
            'FIELD.number.value' => '99',
            '$or'                => [
                { 'TOPICMOVED.by' => 'AlbertCamus' },
                { 'TOPICMOVED.by' => qr/(?-xism:.*bert.*)/ }
            ],
            'FIELD.string.value' => 'String'
        }
    );
}

sub test_hoistCompound2 {
    my $this = shift;
    my $s =
"(moved.by='AlbertCamus' OR moved.by ~ '*bert*') AND number=99 AND string='String'";
    my $queryParser = new Foswiki::Query::Parser();
    my $query       = $queryParser->parse($s);
    my $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);

    $this->do_Assert(
        $query,
        $mongoDBQuery,
        {
            'FIELD.number.value' => '99',
            'FIELD.string.value' => 'String',
            '$or'                => [
                { 'TOPICMOVED.by' => 'AlbertCamus' },
                { 'TOPICMOVED.by' => qr/(?-xism:.*bert.*)/ }
            ]
        }
    );
}

sub test_hoistAlias {
    my $this        = shift;
    my $s           = "info.date=12345";
    my $queryParser = new Foswiki::Query::Parser();
    my $query       = $queryParser->parse($s);
    my $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);

    $this->do_Assert( $query, $mongoDBQuery, { 'TOPICINFO.date' => '12345' } );
}

sub test_hoistFormField {
    my $this        = shift;
    my $s           = "TestForm.number=99";
    my $queryParser = new Foswiki::Query::Parser();
    my $query       = $queryParser->parse($s);
    my $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);

    $this->do_Assert( $query, $mongoDBQuery, { 'FIELD.number.value' => '99' } );
}

sub test_hoistText {
    my $this        = shift;
    my $s           = "text ~ '*Green*'";
    my $queryParser = new Foswiki::Query::Parser();
    my $query       = $queryParser->parse($s);
    my $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);
    $this->do_Assert( $query, $mongoDBQuery,
        { '_text' => qr/(?-xism:.*Green.*)/ } );
}

sub test_hoistName {
    my $this        = shift;
    my $s           = "name ~ 'Web*'";
    my $queryParser = new Foswiki::Query::Parser();
    my $query       = $queryParser->parse($s);
    my $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);

    $this->do_Assert( $query, $mongoDBQuery,
        { '_topic' => qr/(?-xism:Web.*)/ } );
}

sub test_hoistName2 {
    my $this        = shift;
    my $s           = "name ~ 'Web*' OR name ~ 'A*' OR name = 'Banana'";
    my $queryParser = new Foswiki::Query::Parser();
    my $query       = $queryParser->parse($s);
    my $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);

    $this->do_Assert(
        $query,
        $mongoDBQuery,
        {
            '$or' => [
                { '_topic' => qr/(?-xism:Web.*)/ },
                { '_topic' => qr/(?-xism:A.*)/ },
                { '_topic' => 'Banana' }
            ]
        }
    );
}

sub test_hoistOP_Match {
    my $this        = shift;
    my $s           = "text =~ '.*Green.*'";
    my $queryParser = new Foswiki::Query::Parser();
    my $query       = $queryParser->parse($s);
    my $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);

    $this->do_Assert( $query, $mongoDBQuery,
        { '_text' => qr/(?-xism:.*Green.*)/ } );
}


sub test_hoistOP_Where {
    my $this        = shift;
    my $s           = "preferences[name='SVEN']";
    my $queryParser = new Foswiki::Query::Parser();
    my $query       = $queryParser->parse($s);
    my $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);

# db.current.find({ 'PREFERENCE.__RAW_ARRAY' : { '$elemMatch' : {'name' : 'SVEN' }}})

    $this->do_Assert( $query, $mongoDBQuery,
        { 'PREFERENCE.__RAW_ARRAY' => { '$elemMatch' => {'name' => 'SVEN' }}}
        );
}
#i think this is meaninless, but i'm not sure.
sub test_hoistOP_preferencesDotName {
    my $this        = shift;
    my $s           = "preferences.name='BLAH'";
    my $queryParser = new Foswiki::Query::Parser();
    my $query       = $queryParser->parse($s);
    my $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);

    $this->do_Assert( $query, $mongoDBQuery,
        {  'PREFERENCE.name' => 'BLAH' } );
}

sub test_hoistORANDOR {
    my $this        = shift;
    my $s           = "(number=14 OR number=12) and (string='apple' OR string='bana')";
    my $queryParser = new Foswiki::Query::Parser();
    my $query       = $queryParser->parse($s);
    my $mongoDBQuery =
      Foswiki::Plugins::MongoDBPlugin::HoistMongoDB::hoist($query);

    $this->do_Assert(
        $query,
        $mongoDBQuery,
{
          'FIELD.number.value' => {
                                    '$in' => [
                                               '14',
                                               '12'
                                             ]
                                  },
          'FIELD.string.value' => {
                                    '$in' => [
                                               'apple',
                                               'bana'
                                             ]
                                  }
        }
    );
}

1;
