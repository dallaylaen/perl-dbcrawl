package My::TableFetch;

use strict;
use warnings;

sub new {
	my ($class, %opt) = @_;

	my $self = bless {
		uniq       => {},
		table_key  => {},
		links      => {},
		dbh        => $opt{dbh},
	}, $class;
	
	return $self;
};

sub add_table {
	my ($self, %opt) = @_;

	my $table = $opt{table};
	my $key   = $opt{key};
	# TODO die unless

	$self->{table_key}{ $table } = $key;
	$self->{uniq}{ $table } = {};
	return $self;
};

sub add_link {
	my ($self, %opt) = @_;

	# TODO die unless
	my ($from_table, $from_key, $to_table, $to_key)
		= @opt{ qw{ from_table from_key to_table to_key} };
	$to_key ||= $from_key;

	push @{ $self->{links}{ $from_table } },
		[ $from_key, $to_table, $to_key ];

	return $self;
};

sub add_data {
	my ($self, %opt) = @_;

	my $row   = $opt{data};
	my $table = $opt{table};

	my $key   = $self->{table_key}{ $table };

	die "Uknown table $table"
		unless $key;

	my $id = $row->{$key};
	die "Key missing in record"
		unless defined $id;

	if ($self->{uniq}{ $table }{ $id }) {
		# avoid looping
		# TODO die if data mismatch
		return;
	};

	$self->{uniq}{ $table }{ $id } = $row;
	
	my @ret;
	my $links = $self->{links}{ $table } || [];
	foreach (@$links) {
		my ($from_key, $to_table, $to_key) = @$_;

		my $id = $row->{$from_key};
		warn "in links: $table:$from_key => $to_table:$to_key; id="
			.($id//'(undef)');
		defined $id or next;

		# TODO check if already fetched
		# maybe only when to_key is PK (otherwise may be moar records)

		push @ret, {
			table => $to_table,
			key => $to_key,
			value => $id,
		};
	};

	return @ret; # next assignments to process
};

sub fetch_rows {
	my ($self, %opt) = @_;

	# TODO check params

	use warnings FATAL => qw(uninitialized);
	my $sth = $self->{dbh}->prepare_cached(
		"SELECT * FROM $opt{table} WHERE $opt{key} = ?" );

	$sth->execute( $opt{value} );

	my @ret;
	while (my $row = $sth->fetchrow_hashref) {
		push @ret, { table => $opt{table}, data => $row };
	};
	$sth->finish;

	warn "fetch_rows: got ".(scalar @ret)." rows";

	return @ret;
};

sub do_fetch {
	my ($self, @queue) = @_;

	while (@queue) {
		my @rows = $self->fetch_rows( %{ shift @queue } );
		foreach (@rows) {
			push @queue, $self->add_data( %$_ );
		};
	};

	return $self;
};

sub get_data {
	my $self = shift;

	my @ret; # gonna be huge - TODO replace return w/print or callback
	foreach my $table (keys %{ $self->{uniq} }) {
		my $key  = $self->{table_key}{ $table };
		my $data = $self->{uniq}{ $table };
		foreach (values %$data) {
			push @ret, { table => $table, key => $key, data => $_ };
		};
	};

	return @ret;
};

sub ifsert_row {
	my ($self, %opt) = @_;

	# TODO check input
	my $table = $opt{table};
	my $key   = $opt{key};
	my $data  = $opt{data};
	my $id    = $data->{$key};

	my $sth_sel = $self->{dbh}->prepare_cached(
		"SELECT 1 FROM $table WHERE $key = ?");
	my $rows = $sth_sel->execute($id);
	$sth_sel->finish; # don't need data

	# it's there - skip.
	if ($rows >= 1) {
		return;
	};

	my @fields = keys %$data;
	my @values = map { $data->{$_} } @fields;
	my $quest = join ",", ("?") x @fields;
	my $field_list = join ",", @fields;

	my $sth_ins = $self->{dbh}->prepare_cached(
		"INSERT INTO $table ($field_list) VALUES ($quest);");

	my $rows = $sth_ins->execute( @values );
	# TODO $rows != 1 - bad!!!
	$sth_ins->finish;

	return "DELETE FROM $table WHERE $key = ".$self->{dbh}->quote($id).";";
};

1;
