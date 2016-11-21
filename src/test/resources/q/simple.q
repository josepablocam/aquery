//	Section 5: Tests from Alberto Lerner's thesis and presentation on aquery.
//	We assume the existence of tables: Ticks, Portfolio, Packets, Sales, TradedStocks,
// HistoricQuotes as per references in both documents


//Test 5.1: pg 24
.kdb.q0:{select price from `timestamp xasc Ticks where ID=`ACME}


//Test 5.2: pg 25, example 3.1
//naively writtenm
.kdb.q1:{select max_profit:max price-mins price from `timestamp xasc Ticks where ID=`ACME, date=pointDate}

//Test 5.3: pg 29, example 3.3
.kdb.q2:{select src, dest, len, ct from
  select len:avg length, ct:count timestamp by src, dest, sts:sums (120*1e9)<deltas timestamp
  from `src xasc `dest xdesc `timestamp xasc Packets
  }


//Test 5.4: pg 30, example 3.4
.kdb.q3:{
    as:select date, timestamp, a21:21 mavg price, a5:5 mavg price by ID from `ID`timestamp xasc Ticks;
    select ID, date from `ID`timestamp xasc ungroup as where (a21>a5)&(prev[a21]<=prev a5)&(prev ID)=ID
    }


//Test 5.5: pg 32, example 3.5
//recreating innerjoin semantics from traditional sql
.kdb.q4:{0!select last_price:neg[10] sublist price by ID from `timestamp xasc ej[`ID;Ticks;Portfolio]}


//Test 5.6: pg 33, example 3.6
.kdb.q5:{
     OneDay:select ID, price, timestamp from `timestamp xasc Ticks where date=pointDate;
     select ct:count i by ID from `timestamp xasc OneDay where i < 1000
 }

//Test 5.7: moving average over arrables
.kdb.q6:{select month, moving_sales:3 mavg sales from `month xasc Sales}


//Test 5.8:
//recreating innerjoin semantics from traditional sql
.kdb.q7:{select avg_price:10 mavg ClosePrice by ID from `date xasc ej[`ID`date;`ID`date xcol TradedStocks;`ID xcol HistoricQuotes]}

.kdb.q8:{select last_price:last price from `name`timestamp xasc ej[`ID;base;Ticks] where name=`x}

// testing push filters
.kdb.q9:{select ID, date, timestamp, price from ej[`ID;TicksWithAttr;Portfolio] where i < 1000}

.kdb.pythag:{sqrt (x*x)+y*y}
.kdb.q10:{ .kdb.pythag[3;4]}

.kdb.q11:{ (`c1 xasc t2) upsert `c2`c1!-1 -2 }
.kdb.q12:{
    0!update c3:last sums c2 by c1, c2 from `c2 xasc tu1 where c1 < 3, 2 <=(count;i) fby ([]c1;c2);
  }
.kdb.q13:{ update c3:max c2 from tu2 where c1 > 0 }
.kdb.q14:{ delete from `c2 xasc tu1 where c1 < 3, 1<(count;i) fby c1 }
.kdb.q15:{ delete c2 from tu2 }


