/
  Base library for aquery code, version 1.0
  Please report any bugs to jpc485@nyu.edu
\

// Translation utilities
// initialize state
.aq.initQueryState:{.aq.cd:s!s:`$(); .aq.pc:s;};

// Generate column dictionary
.aq.gencd:{[t;sn;rename]
  prefix:$[rename;sn,"__";""];
  $[0=count sn;
    c!c:cols t;
    ((`$(sn,"."),/:sc),c)!(2*count c)#`$prefix,/:sc:string c:cols t
    ]
   };

// rename columns using a dictionary
.aq.drcols:{[t;d](c^d c:cols t) xcol t};
// rename columns using a prefix
.aq.rcols:{[t;p] $[0=count p; t; .aq.drcols[t;c!`$(p,"__"),/:string c:cols t]]};

// initialize table and add to relevant global info
.aq.initTable:{[t;nm;rename]
   // given tables column dictionary
   tc:.aq.gencd[t;nm;rename];
   //drop ambiguous cols from map
   .aq.cd:{(key[y] inter .aq.pc) _ x,y}[.aq.cd;tc];
   // add original columns to .aq.pc
   .aq.pc:.aq.pc union cols t;
   // return table w/ renamed columns
   $[rename;.aq.rcols[t;nm];t]
 };

.aq.initState:{.aq.gencd:{x!x}`$(); .aq.pc:`$();};

// sort column names by attribute
.aq.scbix:{[m] iasc `s`p`g`u?exec c!a from m};
.aq.swix0:{[c;w]
  w iasc min each c?fw@'where each type[`]=(type each) each fw:(raze/) each w
 };
// reorder where clauses based on indices
.aq.reorderFilter:{[v;w]
 // no point in reording if we don't have attributes
 // otherwise reorder locally between aggregates according to safe principle
 $[exec all null a from m:meta v; w; .aq.swix0[.aq.scbix m;w]]
 };

// sort a table by direction-column tuples d and sort only columns c
.aq.sort:{[t;d;c]
 if[0h<>type first d;'"must be list of tuples of direction and column"];
 ix:{[t;ix;dc] ix dc[0] (t dc[1]) ix}[t;;]/[::;reverse d];
 cl:(),c;
 @[t;cl inter $[99h=type t;key t;cl];@[;ix]]
 }

// sort a table when it is grouped (handles group columns appropriately)
.aq.sortGrouped:{[tg;d;c]
  k:keys tg;
  // remove tuples involving grouping direction. semantics indicate grouping is after sort
  // so new order can be imposed via grouping e.g. select * by c1, c3 from `c1 xasc `c3 xdesc t
  dc:d where not (last each d) in k;
  $[(0=count tg)|0=count dc;tg;.aq.sort[ ;dc;c] each tg]
 }


// join using preparation
.aq.joinUsingPrep:{[cs;j]
  $[2<>count m:cs where cs like "*__",s:string j;
    '"ambig-join:",s;
    `rename`remap!(m!2#n;((.aq.cd?m),j)!(1+count m)#n:`$"_"sv string m)
    ]
  };

.aq.joinUsing:{[jf;l;r;cs]
  // join using information
  jui:raze each flip .aq.joinUsingPrep[cols[l],cols r;] each (),cs;
  // remap column references
  .aq.cd,:jui`remap;
  l:.aq.drcols[l;jui`rename];
  r:.aq.drcols[r;jui`rename];
  jf[.aq.cd cs;l;r]
 }

//full outer join using (definition compliant with traditional sql semantics
// from ej
k).aq.ejix:{(=x#z:0!z)x#y:0!y};
.aq.foju:{
  nix:.aq.ejix[x:(),x;y:0!y;z:0!z];
  iz:raze nix; //indices in z for equijoin
  iy:where count each nix; // indices in y for equijoin
  ejr:y[iy],'(x _ z) iz; // perform equi join
  my:select from y where not i in iy; // records in y not in equi join
  mz:select from z where not i in iz; // records z not in equi join
  ejr upsert/(my;mz) // add missing records
  };
// nested join
.aq.nj:{[t1;t2;p] raze {?[x,'count[x]#enlist y;z;0b;()]}[t1; ;p] each t2};

// hash join
.aq.hj:{[t1;t2;a1;a2;p]
  // argument preparation
  a1,:();a2,:();p:$[0<>type first p;enlist p;p];hasneq:any not (=)~/:first each p;
  targs:$[count[t2]>count t1;(t1;t2;a1;a2);(t2;t1;a2;a1)];
  s:targs 0;b:targs 1;sa:targs 2;ba:targs 3;
  // here the "hash function" is identity of join attributes, extract index
  bti:?[s;();sa!sa;`i];
  // hash larger and drop no matches
  bw:?[b;();ba!ba;`i];
  matches:((sa xcol key bw) inter key bti)#bti;
  // perform nj for all matches using complete join predicate
  // if has any predicate that is not equality based otherwise just cross (guaranteed matches)
  inner:b bw ba xcol key matches;
  outer:s value matches;
  $[hasneq;raze .aq.nj'[inner;outer;(count matches)#enlist p];raze {x cross y}'[inner;outer]]
 };

// check if tables are keyed on join keys
// if so use ij instead of ej, much more performant, same semantics in such a case
.aq.iskey:{(count[k]>0)&min (k:keys x) in y};

// faster equi join based on keyed or not
.aq.ej:{[k;t1;t2] $[(kt2:.aq.iskey[t1;k])|kt1:.aq.iskey[t2;k]; $[kt1;t1 ij t2;t2 ij t1]; ej[k;t1;t2]]};

// check attributes
.aq.chkattr:{[x;t] any (.aq.cd where any each flip .aq.cd like/: "*",/:string (),x) in exec c from meta t where not null a};

// enlist for variables inside functions
.aq.funEnlist:{$[0>type x;x;enlist x]};

.aq.wildCard:{{x!x} cols x};

// load
.aq.load:{[fileh;sep;destnm]
  data:(upper exec t from meta destnm;enlist sep) 0:hsym fileh;
  destnm upsert data
  }
.aq.save:{[fileh;sep;t] fileh 0:sep 0:t};

.aq.insert:{[tnm;sorted;modifier;src]
  ctnm:cols tnm;
  if[(0 < count modifier) & count[modifier]<>count ctnm;'"explicitly state all cols"];
  l:$[0 < count modifier; modifier; ctnm];
  d:$[98<=type src;l xcol src;l!src];
  tnm set sorted upsert d
  };

// case expression
.aq.else:{$[0=count x;first 0#y;x]};
// explicit conditions
.aq.eCond:{[ct;e] ?[ct[0;0]; ct[0;1]; $[1=count ct; .aq.else[e; ct[0;1]]; .z.s[1_ct;e]]]};
// implicit conditions
.aq.searchedCond:{[v;ct;e] .aq.eCond[flip (eval each (=;v; ) each first fct; last fct:flip ct); e]};
// wrapper
.aq.cond:{[v;ct;e] $[0=count v; .aq.eCond[ct;e]; .aq.searchedCond[v;ct;e]]}


// Builtins
// Overloaded built-ins are disambiguated by appending the number of arguments
// This precludes overloads with same number of args but different argument types If this is desired
// then it has to be handled at runtime.
.aq.abs:abs;
.aq.and:min;
.aq.avg:avg;
.aq.avgs1:avgs;
.aq.avgs2:mavg;
.aq.between:{x within (y;z)};
.aq.concatenate:(upsert/);
.aq.count:count;
.aq.deltas:deltas;
.aq.distinct:distinct;
.aq.drop:_;
.aq.list:{$[0<=type x;x;enlist x]};
.aq.exec_arrays:{show {key[x] set'value x}flip 0!x;x};
.aq.flatten:ungroup;
.aq.fill:^;
.aq.first1:first;
.aq.first2:sublist;
.aq.is:(::);
.aq.in:in;
.aq.indexEven:{x where (count x)#10b};
.aq.indexOdd:{x where (count x)#01b};
.aq.indexEveryN:{y where $[0>=x;();(count y)#((x-1)#0b),1b]};
.aq.last1:last;
.aq.last2:{[x;y] neg[x] sublist y};
.aq.like:{[x;y] x like string y};
.aq.makeNull:first 0#;
.aq.max:max;
.aq.maxs1:maxs;
.aq.maxs2:mmax;
.aq.min:min;
.aq.mins1:mins;
.aq.mins2:mmin;
.aq.mod:mod;
.aq.moving:{[f;w;a] f each {(x sublist y),z}[1-w;;]\[a]};
.aq.neg:neg;
.aq.next1:next;
.aq.next2:{(neg x) xprev y};
.aq.not:not;
.aq.null:null;
.aq.or:max;
.aq.overlaps:{[x;y] not (x[1]<y[0])|y[1]<x[0]};
.aq.pow:xexp;
.aq.prev1:prev;
.aq.prev2:xprev;
.aq.prd:prd;
.aq.prds:prds;
.aq.ratios:{[x;y] y%x xprev y};
.aq.reverse:reverse;
.aq.show:show;
.aq.sum:sum;
.aq.sums1:sums;
.aq.sums2:msum;
.aq.sqrt:sqrt;
.aq.stddev:dev;
.aq.toSym:`$;
.aq.vars:{mavg[x;y*y]-m*m:mavg[x;y:"f"$y]};