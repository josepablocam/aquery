
.kdb.q0:{select from df_frame where bool_arr}

.kdb.q1:{
 0!select value1:avg(value1), value2:var(value1), value3:sum(value3) by key1, key2 from df_groupby
 }

.kdb.q2:{0!select ct:count i by val from s_value_counts}

.kdb.q3:{
  0!select val:sum(data1) by key1, key2 from df_groupby_multi_python
  }

.kdb.q4:{select from mdf1,mdf2}

.kdb.q5:{
  select key1, key2, A, B, C, D, data1, data2 from ej[`key1`key2;df_join;df_multi_join]
 }


.kdb.q6:{select std_dev:dev val from s_std}

