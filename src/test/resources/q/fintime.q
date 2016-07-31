.kdb.q0:{
	data:select Id, TradeDate, ClosePrice from price where Id in stock10, TradeDate >= startYear10,
	TradeDate <= startYear10 + 365 * 10;
	
	`Id`name`bucket xasc (upsert/)
	 {[x;y;z] 
		0!select low:min ClosePrice, high:max ClosePrice, mean:avg ClosePrice 
		by Id, bucket:y TradeDate, name:z from x
		}[data;;]'[(getWeek;getMonth;getYear);`weekly`monthly`yearly]
	}

.kdb.q1:{
	pxdata:select from price where Id in stock1000, TradeDate >= start300Days, 
    TradeDate <= start300Days + 300;
	splitdata:select from split where Id in stock1000, SplitDate >= start300Days;
  adjdata:select adjFactor:prd SplitFactor by Id, TradeDate
    from ej[`Id;pxdata;splitdata] where TradeDate < SplitDate;
  
    0!`Id`TradeDate xasc
    select Id, TradeDate, HighPrice:HighPrice*1^adjFactor, 
    LowPrice:LowPrice*1^adjFactor, 
    ClosePrice:ClosePrice*1^adjFactor,
    OpenPrice:OpenPrice*1^adjFactor,
    Volume:Volume%1^adjFactor
    from pxdata lj `Id`TradeDate xkey adjdata
	}  

.kdb.q2:{
	pxdata:select Id, TradeDate, HighPrice, LowPrice from price where Id in stock1000, 
		TradeDate within (startPeriod;endPeriod);
	splitdata:select Id, TradeDate:SplitDate, SplitFactor from split where Id in stock1000, 
		SplitDate within (startPeriod;endPeriod);
	select Id, TradeDate, MaxDiff:HighPrice - LowPrice from 
    `Id`TradeDate xasc ej[`Id`TradeDate;pxdata;splitdata]
	}

.kdb.q3:{
	select avg_close_price:avg ClosePrice from price where Id in SP500, TradeDate = startPeriod
 }

.kdb.q4:{
	select avg_close_price:avg ClosePrice from price where Id in Russell2000, TradeDate = startPeriod
 }

.kdb.q5:{
	pxdata:select Id, TradeDate, ClosePrice from price where Id in stock1000, TradeDate >= start6Mo,
	 TradeDate < start6Mo + 31 * 6;
	splitdata:select Id, SplitDate, SplitFactor from split where Id in stock1000, 
    SplitDate >= start6Mo;
	splitadj:0!select ClosePrice:first ClosePrice*prd SplitFactor by Id, TradeDate from 
    ej[`Id;pxdata;splitdata] where TradeDate < SplitDate;
  
  update m21:21 mavg ClosePrice, m5:5 mavg ClosePrice by Id from 
    `Id`TradeDate xasc pxdata lj `Id`TradeDate xkey splitadj  
 };

.kdb.q6:{
	pxdata:select Id, TradeDate, ClosePrice from price where Id in stock1000, TradeDate >= start6Mo,
	 TradeDate < start6Mo + 31 * 6;
	splitdata:select Id, SplitDate, SplitFactor from split where Id in stock1000, 
    SplitDate >= start6Mo;
	splitadj:0!select ClosePrice:first ClosePrice*prd SplitFactor by Id, TradeDate from 
    ej[`Id;pxdata;splitdata] where TradeDate < SplitDate;
  movingAvgs:update m21:21 mavg ClosePrice, m5:5 mavg ClosePrice by Id from 
      `Id`TradeDate xasc pxdata lj `Id`TradeDate xkey splitadj;
    
	select Id, CrossDate:TradeDate, ClosePrice from movingAvgs where Id=prev Id, 
	((prev[m5] <= prev m21) & m5 > m21)|((prev[m5] >= prev m21) & m5 < m21)
  }

.kdb.q7:{
 alloc:10000.0;
 pxdata:select Id, TradeDate, ClosePrice from price where Id in stock10, 
  TradeDate >= -365 + max TradeDate;
 
 splitdata:select Id, SplitDate, SplitFactor from split where Id in stock10, 
   SplitDate >= -365 + max SplitDate;
 
 splitadj:0!select ClosePrice:first ClosePrice*prd SplitFactor by Id, TradeDate from 
   ej[`Id;pxdata;splitdata] where TradeDate < SplitDate;
 
 adjpxdata:`Id`TradeDate xasc pxdata lj `Id`TradeDate xkey splitadj;
 
 movingAvgs:update m21day:21 mavg ClosePrice, m5month:160 mavg ClosePrice by Id from adjpxdata;
 
 simulated:select result:alloc*prd ?[maxs m21day > m5month;?[m21day > m5month;1%ClosePrice;ClosePrice];1],
   stillInvested:last[m21day] > last m5month by Id
   from movingAvgs where (Id=prev[Id]) & 
    (
      ((prev[m5month] <= prev m21day)& m5month > m21day) |
      ((prev[m5month] >= prev m21day)& m5month < m21day)
    );
   
 latestPxs:select Id, ClosePrice from adjpxdata where TradeDate=max TradeDate;
 select stock_value:sum alloc^result * ?[stillInvested;ClosePrice;1] from latestPxs lj `Id xkey simulated
 }


.kdb.q8:{
	pricedata:select Id, ClosePrice from `Id`TradeDate xasc price where Id in stock10, TradeDate >= startYear10,
    TradeDate <= startYear10 + 365 * 2;
	pair1:select ClosePrice1:ClosePrice by Id1:Id from pricedata;
	pair2:`Id2`ClosePrice2 xcol pair1;
	// full matrix, not just lower/upper triangular
	corrdata:pair1 cross pair2;
	corrResults:select Id1, Id2, corrCoeff:cor'[ClosePrice1;ClosePrice2] from corrdata where Id1<>Id2;
  `corrCoeff xasc corrResults
 }

.kdb.q9:{
  startDate:first exec (-365*3)+max TradeDate from price;
  startYear:getYear startDate;
  hadSplits:exec distinct Id from split where Id in Russell2000, 
    (getYear SplitDate)>=startYear;
    
  nosplit:select avg_px:avg ClosePrice by Id, year:getYear TradeDate from price 
    where Id in Russell2000, TradeDate>=startDate, not Id in hadSplits;
  
  divdata:select total_divs:sum DivAmt by Id, year:`year$AnnounceDate from dividend 
    where Id in Russell2000, (getYear AnnounceDate)>= startYear, 
    not Id in hadSplits;
  
  0!update yield:total_divs%avg_px from select from nosplit ij `Id`year xkey divdata
 }





