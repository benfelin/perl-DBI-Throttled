package DBI::Throttled;
use strict;
use warnings;
use base 'DBI';

=head1 NAME

DBI::Throttled - DBI subclass with throttling on L<DBI::st/execute>

=head1 DESCRIPTION

=head2 Subclassing

Per L<DBI/Subclassing the DBI>.

=cut


package DBI::Throttled::db;
use strict;
use warnings;
use base 'DBI::db';

use Time::HiRes qw( gettimeofday tv_interval usleep );

sub Duty {
    return 0.001;
}

sub _slow_wrap {
    my ($self, $method, $obj, $code, @arg) = @_;

    my $t0 = [ gettimeofday() ];
    my @rv;
    if (wantarray) {
        @rv = $obj->$code(@arg);
    } else {
        $rv[0] = $obj->$code(@arg);
    }
    my $tq = tv_interval($t0);

    my $duty = $self->Duty;
    my $tdelay = $tq * (1 - $duty) / $duty;
    warn sprintf("%s: tq=%.4fs => tdel=%.4fs\n", $method, $tq, $tdelay);
    usleep($tdelay * 1E6);

    return @rv if wantarray;
    return $rv[0];
}

foreach my $method (qw( do selectall_arrayref selectall_array selectrow_arrayref selectrow_array )) {
    my $code = sub {
        my ($self, @arg) = @_;
        return $self->_slow_wrap
          ($method => $self, $self->can("SUPER::$method"), @arg);
    };
    no strict 'refs';
    *{$method} = $code;
}


package DBI::Throttled::st;
use strict;
use warnings;
use base 'DBI::st';

foreach my $method (qw( execute fetchall_arrayref )) {
    my $code = sub {
        my ($self, @arg) = @_;
        return $self->{Database}->_slow_wrap
          ("st:$method" => $self, $self->can("SUPER::$method"), @arg);
    };
    no strict 'refs';
    *{$method} = $code;
}


1;
