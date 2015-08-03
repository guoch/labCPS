insert overwrite table lineitem_lab partition (SHIPDATE='1997-09-02',DISCOUNT=0.05,SHIPMODE='MAIL') select * FROM lineitem_tmp where (L_SHIPDATE>='1994-01-01' and L_SHIPDATE<'1997-09-02') and L_DISCOUNT<0.05 and L_SHIPMODE='MAIL';

insert overwrite table lineitem_lab partition (SHIPDATE='1997-09-02',DISCOUNT=0.05,SHIPMODE='SHIP') select * FROM lineitem_tmp where (L_SHIPDATE>='1994-01-01' and L_SHIPDATE<'1997-09-02') and L_DISCOUNT<0.05 and L_SHIPMODE='SHIP';

insert overwrite table lineitem_lab partition (SHIPDATE='1997-09-02',DISCOUNT=0.05,SHIPMODE='OTHER') select * FROM lineitem_tmp where (L_SHIPDATE>='1994-01-01' and L_SHIPDATE<'1997-09-02') and L_DISCOUNT<0.05 and (L_SHIPMODE!='SHIP' and L_SHIPMODE!='MAIL' );

insert overwrite table lineitem_lab partition (SHIPDATE='1997-09-02',DISCOUNT=0.07,SHIPMODE='MAIL') select * FROM lineitem_tmp where (L_SHIPDATE>='1994-01-01' and L_SHIPDATE<'1997-09-02') and (L_DISCOUNT>=0.05 and L_DISCOUNT<0.07) and L_SHIPMODE='MAIL';

insert overwrite table lineitem_lab partition (SHIPDATE='1997-09-02',DISCOUNT=0.07,SHIPMODE='SHIP') select * FROM lineitem_tmp where (L_SHIPDATE>='1994-01-01' and L_SHIPDATE<'1997-09-02') and (L_DISCOUNT>=0.05 and L_DISCOUNT<0.07) and L_SHIPMODE='SHIP';

insert overwrite table lineitem_lab partition (SHIPDATE='1997-09-02',DISCOUNT=0.07,SHIPMODE='OTHRT') select * FROM lineitem_tmp where (L_SHIPDATE>='1994-01-01' and L_SHIPDATE<'1997-09-02') and (L_DISCOUNT>=0.05 and L_DISCOUNT<0.07) and (L_SHIPMODE!='SHIP' and L_SHIPMODE!='MAIL' );

insert overwrite table lineitem_lab partition (SHIPDATE='1997-09-02',DISCOUNT=0.09,SHIPMODE='MAIL') select * FROM lineitem_tmp where (L_SHIPDATE>='1994-01-01' and L_SHIPDATE<'1997-09-02') and L_DISCOUNT>=0.07 and L_SHIPMODE='MAIL';

insert overwrite table lineitem_lab partition (SHIPDATE='1997-09-02',DISCOUNT=0.09,SHIPMODE='SHIP') select * FROM lineitem_tmp where (L_SHIPDATE>='1994-01-01' and L_SHIPDATE<'1997-09-02') and L_DISCOUNT>=0.07 and L_SHIPMODE='SHIP';

insert overwrite table lineitem_lab partition (SHIPDATE='1997-09-02',DISCOUNT=0.09,SHIPMODE='OTHER') select * FROM lineitem_tmp where (L_SHIPDATE>='1994-01-01' and L_SHIPDATE<'1997-09-02') and L_DISCOUNT>=0.07 and (L_SHIPMODE!='SHIP' and L_SHIPMODE!='MAIL' );