// must define DATAPATH before running
paths:` sv/:(hsym `$DATAPATH),/:`$("base.psv";"price.psv";"split.psv";"dividend.psv");
files:`base`price`split`dividend!paths;
// Hist base file
base:("SSSSSSD";enlist "|") 0:files`base;
// Hist price file
price:("SDFFFFJ";enlist "|") 0:files`price;
// Hist split file
split:("SDDI";enlist "|") 0:files`split;
// Hist dividends file
dividend:("SDFD"; enlist "|") 0:files`dividend;

// create random sets required for tests a
stock10:neg[10 & count base]?(base`Id);
startYear10:first 1?exec distinct TradeDate from price where TradeDate.year <= -10+max TradeDate.year;
// adding to date here to deal easily with monetdb
endYear10:startYear10 + 365 * 10;
startYear10Plus2:startYear10 + 365 * 2;
stock1000:neg[1000 & count base]?base`Id;
start300Days:first 1?-300_exec asc distinct TradeDate from price;
end300Days:start300Days + 300;
startPeriod:first 1?exec asc distinct TradeDate from price;
endPeriod:first 1?exec distinct TradeDate from price where TradeDate > startPeriod;
SP500:neg[500 & count base]?base`Id;
start6Mo:first 1?exec distinct TradeDate from price where TradeDate.month <= -6 + max TradeDate.month;
end6Mo:start6Mo + 6 * 31;
Russell2000:neg[2000 & count base]?base`Id;
maxTradeDate:exec max TradeDate from price;
maxTradeDateMinusYear:maxTradeDate-365;
maxTradeDateMinus3Years:maxTradeDate-3*365;


getMonth:{1 + (`month$x) mod 12};
getYear:{`year$x};
firstDateOfYear:{`date$`month$d-30*-1+getMonth d:`date$`month$x};
getWeek:{1 + floor (x - firstDateOfYear x)%7};

// type casting to wrap annoying type info loss for empty grouped tables
float:{`float$x}