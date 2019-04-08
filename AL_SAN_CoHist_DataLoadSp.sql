CREATE DEFINER=`san_intelli`@`%` PROCEDURE `AL_SAN_CoHist_DataLoadSp`()
BEGIN

	DECLARE 			done			  					  INT DEFAULT FALSE;
    DECLARE 			Err				  					  INT DEFAULT FALSE;
  	DECLARE             VSiteRef                               varchar(8) ;
	DECLARE             Vwhse                                  varchar(4) ;
	DECLARE             Vitem                                  varchar(30) ;
	DECLARE             Vrank                                  varchar(4) ;
	DECLARE             Vloc                                   varchar(15) ;
	DECLARE             Vqty_on_hand                           decimal(19,8) ;
	DECLARE             Vmrb_flag                              varchar(4) ;
	DECLARE             Vloc_type                              varchar(4) ;
	DECLARE             Vqty_rsvd                              decimal(19,8);
	DECLARE             Vwc                                    varchar(6);
	DECLARE             VCreateDate                            datetime;
	DECLARE             Vunit_cost                             decimal(20,8);
	DECLARE             Vcust_num                              varchar(7);
	DECLARE             Vcust_seq                              varchar(10);
	DECLARE             Vcust_type                             varchar(6);
	DECLARE             Vname                          		   varchar(60);
	DECLARE             Vaddr1                         		   varchar(50);
	DECLARE             Vaddr2                          	   varchar(50);
	DECLARE             Vaddr3                          	   varchar(50);
	DECLARE             Vaddr4                          	   varchar(50);
	DECLARE             Vcity                          		   varchar(30);
	DECLARE             Vstate                          	   varchar(5);
	DECLARE             Vzip                          		   varchar(10);
	DECLARE             Vcounty                                varchar(30);
	DECLARE             Vcountry                               varchar(30);
	DECLARE             Vcurr_code                             varchar(4);
	DECLARE             Vcorp_cust                             varchar(7);
	DECLARE             Vend_user_type                         varchar(6);
	DECLARE             Vterms_code                            varchar(4);
	DECLARE             Vship_code                             varchar(4);
	DECLARE             Vbankcode                              varchar(4);
	DECLARE             Vpaytype                          	   varchar(4);
	DECLARE             Vpricecode                             varchar(4);
	DECLARE             Vtype                                  varchar(4);
	DECLARE             Vco_num                          	   varchar(10);
	DECLARE             Vest_num                          	   varchar(10);
	DECLARE             Vcontact                          	   varchar(30);
	DECLARE             Vphone                          	   varchar(25);
	DECLARE             Vbillto_name                           varchar(60);
	DECLARE             Vbillto_addr1                          varchar(50);
	DECLARE             Vbillto_addr2                          varchar(50);
	DECLARE             Vbillto_addr3                          varchar(50);
	DECLARE             Vbillto_addr4                          varchar(50);
	DECLARE             Vbillto_city                           varchar(30);
	DECLARE             Vbillto_state                          varchar(5);
	DECLARE             Vbillto_zip                            varchar(10);
	DECLARE             Vbillto_county                         varchar(30);
	DECLARE             Vbillto_country                        varchar(30);
	DECLARE             Vcust_po                               varchar(25);
	DECLARE             Vtaken_by                              varchar(15);
	DECLARE             Vco_stat                               varchar(4);
	DECLARE             Vclose_date                            datetime;
	DECLARE             Vslsman                                varchar(8);
	DECLARE             Vorig_site                             varchar(8);
	DECLARE             Vco_line                               varchar(4);
	DECLARE             Vco_release                            varchar(4);
	DECLARE             Vqty_ordered                           decimal(19,8);
	DECLARE             Vqty_ready                             decimal(19,8);
	DECLARE             Vqty_shipped                           decimal(19,8);
	DECLARE             Vqty_packed                            decimal(19,8);
	DECLARE             Vdisc                                  decimal(4,1);
	DECLARE             Vcost                                  decimal(20,8);
	DECLARE             Vprice                                 decimal(20,8);
	DECLARE             Vref_type                              varchar(4);
	DECLARE             Vref_num                          	   varchar(15);
	DECLARE             Vref_line_suf                          smallint(2);
	DECLARE             Vref_release                           smallint(2);
	DECLARE             Vdue_date                              datetime;
	DECLARE             Vship_date                             datetime;
	DECLARE             Vcoitem_stat                           varchar(4);
	DECLARE             Vu_m                          		   varchar(4);
	DECLARE             Vqty_ordered_conv                      decimal(19,8);
	DECLARE             Vprice_conv                            decimal(18,8);
	DECLARE             Vship_site                             varchar(8);
	DECLARE             Vblanket_qty                           decimal(20,8);
	DECLARE             Vcont_price                            decimal(18,8);
	DECLARE             Vcobln_stat                            varchar(4);
	DECLARE             Vblanket_qty_conv                      decimal(20,8);
	DECLARE             Vcont_price_conv                       decimal(18,8);
	DECLARE             Vdate_seq                              varchar(4);
	DECLARE             Vco_ship_date                          int(8);
	DECLARE             Vjob                                   varchar(20);
	DECLARE             Vsuffix                                varchar(4);
	DECLARE             Vjob_date                              datetime;
	DECLARE             Vord_type                              varchar(4);
	DECLARE             Vord_num                               varchar(10);
	DECLARE             Vord_line                              varchar(4);
	DECLARE             Vord_release                           varchar(4);
	DECLARE             Vqty_released                          decimal(19,8);
	DECLARE             Vqty_complete                          decimal(19,8);
	DECLARE             Vqty_scrapped                          decimal(19,8);
	DECLARE             Vstat                          		   varchar(4);
	DECLARE             Vorder_date                            datetime;
    DECLARE             Vobs_date                              datetime;




SET done = FALSE;
SET Err = FALSE;

	BEGIN
	  DECLARE Cocur CURSOR FOR 
		  SELECT  siteref,type,co_num,est_num,cust_num,cust_seq,
				  contact, phone, cust_type, name, `addr##1`, `addr##2` , `addr##3` ,
				  `addr##4`,city ,state ,zip,county ,country ,billto_name,`billto_addr##1`,
				  `billto_addr##2`,`billto_addr##3` ,`billto_addr##4` ,billto_city ,
				  billto_state , billto_zip , billto_county , billto_country,
				  curr_code, corp_cust , end_user_type , cust_po, order_date ,
				  taken_by ,terms_code, ship_code, co_stat ,close_date,
				  slsman,  pricecode, orig_site, co_line, co_release,
				  item , qty_ordered, qty_ready , qty_shipped, qty_packed,
				  disc , cost, price ,ref_type ,ref_num,ref_line_suf ,ref_release,
				  due_date,ship_date,coitem_stat, u_m , qty_ordered_conv,price_conv ,
				  ship_site ,blanket_qty ,cont_price,cobln_stat ,blanket_qty_conv,
                  cont_price_conv,promise_date
		   FROM AL_SAN_Co_Fact;
	   
	 DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
	 DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET Err = TRUE;

	 OPEN Cocur;

	  read_loop: LOOP
		FETCH Cocur INTO 
				  Vsiteref,Vtype,Vco_num,Vest_num,Vcust_num,Vcust_seq,
				  Vcontact, Vphone, Vcust_type, Vname, Vaddr1, Vaddr2 , Vaddr3,
				  Vaddr4,Vcity,Vstate,Vzip,Vcounty,Vcountry, Vbillto_name,Vbillto_addr1,
				  Vbillto_addr2,Vbillto_addr3 ,Vbillto_addr4 ,Vbillto_city ,
				  Vbillto_state , Vbillto_zip , Vbillto_county , Vbillto_country,
				  Vcurr_code, Vcorp_cust , Vend_user_type , Vcust_po, Vorder_date ,
				  Vtaken_by ,Vterms_code, Vship_code, Vco_stat ,Vclose_date,
				  Vslsman,  Vpricecode, Vorig_site, Vco_line, Vco_release,
				  Vitem , Vqty_ordered, Vqty_ready , Vqty_shipped, Vqty_packed,
				  Vdisc , Vcost, Vprice ,Vref_type ,Vref_num,Vref_line_suf ,Vref_release,
				  Vdue_date,Vship_date,Vcoitem_stat, Vu_m , Vqty_ordered_conv,Vprice_conv ,
				  Vship_site ,Vblanket_qty ,Vcont_price,Vcobln_stat ,Vblanket_qty_conv,
                  Vcont_price_conv , Vobs_date;
			  
		IF done THEN
		  LEAVE read_loop;
		END IF;

		IF Err THEN
       			INSERT INTO SAN_ErrorLog (SiteRef,RecType,Err_Date,Err_Desc) VALUES
				(VSiteRef,'COFACT_H',now(),CONCAT('Error on insert / update - ', CONVERT(Vco_num,char), ' - ' , CONVERT(Vco_line,char), ' - ' , CONVERT(Vco_release,char)));
		
        SET Err = FALSE;
        END IF;
        

		IF (SELECT 1 = 1 FROM SAN_Co_Fact WHERE siteref= Vsiteref AND co_num = Vco_num AND co_line = Vco_line AND  co_release = Vco_release) THEN
			
			UPDATE SAN_Co_Fact SET type = Vtype,est_num = Vest_num,cust_num = Vcust_num,cust_seq = Vcust_seq,
									   contact = Vcontact, phone = Vphone, cust_type =Vcust_type, name = Vname, `addr##1` = Vaddr1, `addr##2` =Vaddr2 , `addr##3` = Vaddr3 ,
									   `addr##4` = Vaddr4,city = Vcity ,state = Vstate ,zip = Vzip,county = Vcounty ,country = Vcountry ,billto_name = Vbillto_name,`billto_addr##1` = Vbillto_addr1,
									   `billto_addr##2` = Vbillto_addr2,`billto_addr##3` = Vbillto_addr3,`billto_addr##4` = Vbillto_addr4,billto_city = Vbillto_city ,
									   billto_state = Vbillto_state , billto_zip = Vbillto_zip , billto_county = Vbillto_county , billto_country = Vbillto_country,
									   curr_code = Vcurr_code, corp_cust = Vcorp_cust, end_user_type =Vend_user_type , cust_po = Vcust_po, order_date = Vorder_date ,
									   taken_by = Vtaken_by,terms_code =Vterms_code, ship_code = Vship_code, co_stat = Vco_stat ,close_date = Vclose_date,
									   slsman = Vslsman,  pricecode = Vpricecode, orig_site = Vorig_site, 
									   item = Vitem , qty_ordered = Vqty_ordered, qty_ready = Vqty_ready , qty_shipped = Vqty_shipped, qty_packed = Vqty_packed,
									   disc = Vdisc , cost = Vcost, price = Vprice ,ref_type = Vref_type ,ref_num = Vref_num,ref_line_suf = Vref_line_suf ,ref_release = Vref_release,
									   due_date = Vdue_date,ship_date = Vship_date, coitem_stat = Vcoitem_stat, u_m = Vu_m , qty_ordered_conv = Vqty_ordered_conv,price_conv = Vprice_conv ,
									   ship_site = Vship_site ,blanket_qty = Vblanket_qty ,cont_price = Vcont_price,cobln_stat = Vcobln_stat ,
                                       blanket_qty_conv = Vblanket_qty_conv, cont_price_conv = Vcont_price_conv , promise_date = Vobs_date
                                       ,trn_flag = 0 
						WHERE siteref= Vsiteref AND co_num = Vco_num AND co_line = Vco_line AND
							 co_release = Vco_release; 
        
		 ELSE
      
		      INSERT INTO SAN_Co_Fact (siteref,type,co_num,est_num,cust_num,cust_seq,
									   contact, phone, cust_type, name, `addr##1`, `addr##2` , `addr##3` ,
									   `addr##4`,city ,state ,zip,county ,country ,billto_name,`billto_addr##1`,
									   `billto_addr##2`,`billto_addr##3` ,`billto_addr##4` ,billto_city ,
									   billto_state , billto_zip , billto_county , billto_country,
									   curr_code, corp_cust , end_user_type , cust_po, order_date ,
									   taken_by ,terms_code, ship_code, co_stat ,close_date,
									   slsman,  pricecode, orig_site, co_line, co_release,
									   item , qty_ordered, qty_ready , qty_shipped, qty_packed,
									   disc , cost, price ,ref_type ,ref_num,ref_line_suf ,ref_release,
									   due_date,ship_date,coitem_stat, u_m , qty_ordered_conv,price_conv ,
									   ship_site ,blanket_qty ,cont_price,cobln_stat ,
                                       blanket_qty_conv, cont_price_conv , promise_date) 
                                            
							   VALUES  (Vsiteref,Vtype,Vco_num,Vest_num,Vcust_num,Vcust_seq,
										Vcontact, Vphone, Vcust_type, Vname, Vaddr1, Vaddr2 , Vaddr3,
										Vaddr4,Vcity,Vstate,Vzip,Vcounty,Vcountry, Vbillto_name,Vbillto_addr1,
										Vbillto_addr2,Vbillto_addr3 ,Vbillto_addr4 ,Vbillto_city ,
										Vbillto_state , Vbillto_zip , Vbillto_county , Vbillto_country,
										Vcurr_code, Vcorp_cust , Vend_user_type , Vcust_po, Vorder_date ,
										Vtaken_by ,Vterms_code, Vship_code, Vco_stat ,Vclose_date,
										Vslsman,  Vpricecode, Vorig_site, Vco_line, Vco_release,
										Vitem , Vqty_ordered, Vqty_ready , Vqty_shipped, Vqty_packed,
										Vdisc , Vcost, Vprice ,Vref_type ,Vref_num,Vref_line_suf ,Vref_release,
										Vdue_date,Vship_date,Vcoitem_stat, Vu_m , Vqty_ordered_conv,Vprice_conv ,
										Vship_site ,Vblanket_qty ,Vcont_price,Vcobln_stat ,
                                        Vblanket_qty_conv, Vcont_price_conv, Vobs_date);
		 END IF;          
	  END LOOP;

	 CLOSE Cocur;
	  
  END;

   
#LOAD COSHIPFACT
SET done = FALSE;
SET Err = FALSE;

  BEGIN
    DECLARE Coshipcur CURSOR FOR 
         SELECT DISTINCT siteref,co_num,co_line, co_release
                 FROM AL_SAN_Coship_Fact;
    
      DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
      DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET Err = TRUE;

   OPEN Coshipcur;

   read_loop: LOOP
     FETCH Coshipcur INTO 
              Vsiteref,Vco_num,Vco_line, Vco_release;
     
    IF done THEN
      LEAVE read_loop;
    END IF;

	IF Err THEN
          	INSERT INTO SAN_ErrorLog (SiteRef,RecType,Err_Date,Err_Desc) VALUES
			(VSiteRef,'COSHIPFACT_H',now(),CONCAT('Error on insert / update - ', CONVERT(Vco_num,char), ' - ' , CONVERT(Vco_line,char), ' - ' , CONVERT(Vco_release,char)));

    SET Err = FALSE;
    END IF;
    
		IF EXISTS (SELECT  1 FROM SAN_Coship_Fact WHERE siteref= Vsiteref AND co_num = Vco_num AND co_line = Vco_line 
                            AND co_release = Vco_release) THEN
                              
				BEGIN
					DELETE FROM SAN_Coship_Fact WHERE siteref= Vsiteref AND co_num = Vco_num AND co_line = Vco_line 
										AND co_release = Vco_release;	
                    
				END;
                
		END IF;
        
        
                            
		INSERT INTO SAN_Coship_Fact (siteref, co_num, co_line, co_release, date_seq, ship_date, 
								qty_shipped, cost , price) 

		SELECT  siteref,co_num,co_line, co_release , date_seq, ship_date, qty_shipped, cost , price
		FROM GLT_SAN_Coship_Fact WHERE  siteref= Vsiteref AND co_num = Vco_num AND co_line = Vco_line AND 
				co_release = Vco_release; 
        
    END LOOP;
	CLOSE Coshipcur;
  END;


#Update Sales value by discount - 26-12-2017
BEGIN
  UPDATE  `SAN_Co_Fact` `c`
        JOIN `SAN_Coship_Fact` `s` ON `c`.`SiteRef` = `s`.`SiteRef`
            AND `c`.`co_num`     = `s`.`co_num`
            AND `c`.`co_line`    = `s`.`co_line`
            AND `c`.`co_release` = `s`.`co_release`
	SET `s`.`SaleValue`   = round((`s`.`qty_shipped` * `c`.`price`), 4),
		`s`.`TotLineDisc` = round(((`s`.`qty_shipped` * `c`.`price`) * (`c`.`disc` / 100)),4),
		`s`.`TotHdrDisc`  = 			
		( round((((`s`.`qty_shipped`  *`c`.`price`) - ((`s`.`qty_shipped`  *`c`.`price`) * (`c`.`disc` / 100))) * (`c`.`hdr_disc`/100)),4))
				
	WHERE `c`.`co_num` IN (SELECT DISTINCT co_num  
							FROM AL_SAN_Co_Fact 
							);
END;


#Update Sales value by discount - 26-12-2017
BEGIN
  UPDATE  `SAN_Co_Fact` `c`
        JOIN `SAN_Coship_Fact` `s` ON `c`.`SiteRef` = `s`.`SiteRef`
            AND `c`.`co_num`     = `s`.`co_num`
            AND `c`.`co_line`    = `s`.`co_line`
            AND `c`.`co_release` = `s`.`co_release`
	SET `s`.`Sales` = (SaleValue - TotLineDisc - TotHdrDisc)
		
	WHERE `c`.`co_num` IN (SELECT DISTINCT co_num  
							FROM AL_SAN_Co_Fact);
END;


END