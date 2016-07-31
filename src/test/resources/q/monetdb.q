.kdb.quantile:{[x;y] enlist $[0 <> f:if - i;(f * v i+1) - f * v i;0.0]+v i:floor if:-0.5+%[;100] y*n:count v:asc x};
.kdb.q0:{ select q95:.kdb.quantile[val;95] from nums }
.kdb.q1:{ select q5:.kdb.quantile[val;5] from nums }

