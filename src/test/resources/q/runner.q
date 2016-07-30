

help:{
  1 "Usage: \n";
  1 "q runner.q -aquery <translated.q> -kdb <orig.q>\n";
  1 " -aquery_ns [aquery-namespace] -kdb_ns [kdb-namespace]\n";
  1 " -test <csv of 0arg function names>\n";
 }

// returns 1b if loaded correctly, 0b otherwise
safeload:@[{system "l ",x;1b}; ;{show x;0b}];
saferun0:{@[get x;::;show]};
msg:{1 x,"\n"};


run:{[af;qf]
 msg ssr/["==> running %a vs %q";("%a";"%q");string (af;qf)];
 res:(0N!saferun0 af)~0N!saferun0 qf;
 msg (4#" "),"passed:",string res;
 res
 };



opts:.Q.opt .z.x;
if[any not`test`aquery`kdb in key opts; help[];exit 1];

ascript:first opts`aquery;
qscript:first opts`kdb;
ans:`.aq^`$first opts`aquery_ns;
qns:`.kdb^`$first opts`q_ns;
test:`$trim each "," vs first opts`test;

msg "Files: ",ascript,", ",qscript;

if[not safeload ascript; msg "Failed to load:", ascript; exit 1];
if[not safeload qscript; msg "Failed to load:", qscript; exit 1];

afns:` sv/:ans,/:test;
qfns:` sv/:qns,/:test;
results:run'[afns;qfns];
if[any not results; msg "FAILED"; exit 1];
msg "PASSED";
exit 0;





















