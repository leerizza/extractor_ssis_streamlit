
USE [DWBIBFI2_DWH]
GO
/****** Object:  StoredProcedure [dbo].[Tele_Cust_360_Agreement_20]    Script Date: 27/01/2026 17:26:13 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


  
ALTER PROCEDURE [dbo].[Tele_Cust_360_Agreement_20]         
        
AS        
BEGIN        
        
update UC_DEV..TblStatusJob set RunningStatus = 'R'  
          , StartDate = getdate()        
where subJobName = 'cust360_agreement' ;        
        
-- Agreement_Detail --        
        
IF OBJECT_ID('DWBIBFI2_DWH.dbo.Cust360_Agreement_Temp', 'U') IS NOT NULL  DROP TABLE DWBIBFI2_DWH.dbo.Cust360_Agreement_Temp;        
        
        
  select --count(1)        
   CONVERT(int,(CONVERT(varchar,GETDATE(),112))) as Process_Date,        
   a.CustomerID,        
   a.AgreementNo,        
   a.ApplicationID,        
   a.AgreementDate,        
   a.GoLiveDate,        
   a.MaturityDate,        
   a.RRDDate,        
   a.ContractStatus,        
   a.FundingAmount,        
   fc.SK_Release_Date,        
   fc.SK_ProductOffering,        
   fc.SK_Product,        
   fcss.SK_Contract_Active,        
   a.ProductID,        
   a.ProductOfferingID,        
   a.NTF,        
   a.Tenor,        
   a.EffectiveRate,        
   a.PurposeOfFinancingID,        
   case when a.PurposeOfFinancingID = 'INV' then 'Investasi'        
     when a.PurposeOfFinancingID = 'JAS' then 'Pembiayaan Jasa (Syariah)'        
     when a.PurposeOfFinancingID = 'JBS' then 'Jual Beli (Syariah)'        
     when a.PurposeOfFinancingID = 'PM' then 'Multiguna'        
     when a.PurposeOfFinancingID = 'PMK' then 'Modal Kerja'        
     when a.PurposeOfFinancingID = 'PSY' then 'Pembiayaan Jasa (Syariah)'        
     else 'Other' end as PurposeOfFinancing,        
   a.InstallmentAmount,        
   a.BranchID,        
   b.BranchFullName,        
   ar.AreaFullName,        
   d.WilayahName,        
   a.TotalOTR,        
   convert(numeric(17,8),a.NTF/a.TotalOTR) as LTV_byTotalOTR,        
   a.CreditScore,        
   a.NewApplicationDate,        
   case when (a.NewApplicationDate < '20140201') then 1         
     when (a.CreditScore < 35) then 9         
     when (a.CreditScore < 40) then 8         
     when (a.CreditScore < 43.5) then 7        
     when (a.CreditScore < 47) then 6         
     when (a.CreditScore < 50.5) then 5         
     when (a.CreditScore < 54) then 4         
     when (a.CreditScore < 57.5) then 3         
     when (a.CreditScore >= 57.5) then 2         
     else 1        
   end as Risk_Grade,        
   rg.SK_Risk_Grade_Riil as Risk_Grade_Rill,        
   ao.AnotherLoan,        
   ao.FinancePurpose,        
   ao.CashFlowCoverage,        
   case when ao.FinancePurpose = 'LL' then 'Lain-lain'        
     when ao.FinancePurpose = 'BB' then 'Biaya'        
     when ao.FinancePurpose = 'PA' then 'Pendidikan'        
     when ao.FinancePurpose = 'RR' then 'Renovasi'        
     when ao.FinancePurpose = 'C' then 'Consumtive'        
     when ao.FinancePurpose = 'P' then 'Productive'        
     when ao.FinancePurpose = 'MU' then 'Modal Usaha'        
     when ao.FinancePurpose is null then '-'        
     Else ao.FinancePurpose         
   end as Customer_Purpose,        
   dli.Description Life_Insurance_Coverage,        
   -- add for risk datamart        
   a.OutstandingPrincipal,        
   a.FlatRate,        
   a.FirstInstallment,        
   a.AdminFee,        
   a.AddAdminFee,        
   a.FiduciaFee,        
   a.ProvisionFee,        
   a.NotaryFee,        
   a.ProductType,        
   a.CurrencyID,        
   a.DownPayment,        
   a.ScoringQCA,        
   a.ScoringQCAResult,        
   a.DefaultStatus,        
   a.WODate,        
   a.SupplierID,        
   a.CustomerRatingOnGolive,        
   a.CustomerRatingOnRelease,        
   -- end - add for risk datamart        
   tro.isRO,        
   case when tro.isRO = 1 then 'RO'        
     when tro.isRO = 0 then 'New'        
     else 'Other'         
   end as Customer_Status,        
   am.SOAId,        
   tms.SOA,        
   de.ID_Role,        
   de.ID_Employee,        
   de.Employee,        
   s.SupplierName,        
   dp.Product,        
   ca.[Total Net Income] as CA_TotalNetIncome,        
   ca.[Kepemilikan rumah] as CA_Kepemilikanrumah,        
   ca.[BKR An#] as CA_BKRAn,        
   ca.[total obligasi] as CA_TotalObligasi,        
   ca.bpkbAN as CA_bpkbAN,        
   ca.Experience as CA_Experience,        
   ca.[Tinggal sejak] as CA_TinggalSejak,        
   ca.DSR as CA_DSR,        
   1 as isCurrent  ,      
    /*ench nambah field 20230315 Stefanus*/        
   sa.AssignmentSurveyID,         
   da.Activity ,        
   sa.AssignToSurveyorBy BA  
   --DMD 605  
   , lpd.LastPaidDate  
   , CONVERT(DATE,CONVERT(VARCHAR(8),fc.SK_DueDate),112) AS DueDate  
   , lpd.Installment_Paid  
   , hpd.HighestPassDue  
   , dlt.LTV_Group AS LTV_Real  
   , ps.PefindoResult AS PefindoResultDebitur
   , ISNULL(pc.MobilePhone, cc.MobilePhone) AS MobilePhone  
   , pss.PefindoResult AS PefindoResultPasangan
   --END DMD 605  
  into DWBIBFI2_DWH.dbo.Cust360_Agreement_Temp        
  from DWBIBFI2_STG..STG_Agreement a with (nolock)        
  left join DWBIBFI2_DWH..Fact_Contract fc with (nolock)        
   on a.ApplicationID = fc.ID_Application        
  left join DWBIBFI2_STG..STG_Branch b with(nolock) on b.BranchID = a.BranchID        
  left join DWBIBFI2_STG..STG_TblWilayahArea twa with(nolock) on twa.AreaID = b.AreaID        
  left join DWBIBFI2_STG..STG_TblWilayahMaster d on twa.WilayahID = d.WilayahID        
  left join DWBIBFI2_STG..STG_Area ar with(nolock) on ar.AreaID = b.AreaID        
  left join DWBIBFI2_STG..STG_AgreementOtherData ao with (nolock) on a.ApplicationID = ao.ApplicationID and a.BranchID = ao.BranchID        
  left join DWBIBFI2_DWH..Dim_Life_Insurance_Coverage dli with (nolock) on fc.SK_Life_Insurance_Coverage = dli.SK_Life_Insurance_Coverage        
  left join DWBIBFI2_STG..STG_tblDataROCustomer tro with (nolock) on a.ApplicationID = tro.ApplicationID        
  left join DWBIBFI2_STG..STG_AgreementMarketing am with (nolock) on a.ApplicationID = am.ApplicationID        
  left join DWBIBFI2_STG..STG_TblMasterSOA tms with (nolock) on am.SOAId = tms.SOAId        
  left join DWBIBFI2_DWH..Dim_Employee de with (nolock) on fc.SK_Refferal = de.SK_Employee        
  left join DWBIBFI2_STG..STG_Supplier s with (nolock) on a.SupplierID = s.SupplierID        
  left join DWBIBFI2_DWH..Dim_Product dp with (nolock) on fc.SK_Product = dp.SK_Product        
  left join Risk..CA_PARSE ca with (nolock) on a.AgreementNo = ca.AgreementNo        
   /*ench 20230315 Stefanus*/        
  left join DWBIBFI2_STG..STG_ST_Asset sa with (nolock)   on am.AssignmentSurveyID= sa.AssignmentSurveyID        
  left join DWBIBFI2_DWH..Dim_Activity da with (nolock)   on da.SK_Activity = fc.SK_Activity        
  left join (select ID_Application,SK_Contract_Active   
    from (select row_number() over (partition by ID_Application order by SK_Time desc) as id        
        , ID_Application        
        , SK_Contract_Active        
       from DWBIBFI2_DWH..Fact_Contract_SS with (nolock)) x   
    where id = 1) fcss        
  on a.ApplicationID = fcss.ID_Application        
  left join (SELECT a.ID_Application,        
     a.CreditScoreRiil,        
     CASE WHEN a.SK_Application_Date < 20140201 THEN 1         
       WHEN a.CreditScoreRiil < 35 THEN 9         
       WHEN a.CreditScoreRiil < 40 THEN 8         
       WHEN a.CreditScoreRiil < 43.5 THEN 7         
       WHEN a.CreditScoreRiil < 47 THEN 6         
       WHEN a.CreditScoreRiil < 50.5 THEN 5         
       WHEN a.CreditScoreRiil < 54 THEN 4         
       WHEN a.CreditScoreRiil < 57.5 THEN 3         
       WHEN a.CreditScoreRiil >= 57.5 THEN 2         
       ELSE 1   
     END AS SK_Risk_Grade_Riil,        
     (CEILING(CreditScoreRiil / 10) * 10)UppingCreditScore,        
     ISNULL(dcs.SK_Credit_Scoring_Group,1) AS SK_Credit_Scoring_Group_Riil,        
     dcs.GroupScoringLevel2        
    FROM (SELECT fc.ID_Application,        
        CASE WHEN dr.ID_Region IN ('02','03','05','07','08','15') AND COALESCE(sa.CreditScore,0) <> 0 THEN COALESCE(sa.CreditScore,0)-3.5  
          ELSE COALESCE(sa.CreditScore,0)  
        END AS CreditScoreRiil,        
        FC.SK_Application_Date,  
        fc.SK_Credit_Scoring_Group        
       FROM DWBIBFI2_DWH..Fact_Contract fc with (nolock)        
       LEFT JOIN DWBIBFI2_STG.dbo.STG_Agreement sa with (nolock) ON fc.ID_Application=sa.ApplicationID        
       LEFT JOIN DWBIBFI2_DWH..Dim_Region dr with (nolock) ON fc.SK_Region=dr.SK_Region) a        
    --WHERE SA.IsExpired=0        
    LEFT JOIN (select ParamID as ParamID_Credit_Scoring_Group,        
          cast(Value2 as bigint) as Value2        
      from DWBIBFI2_ODS.dbo.Parameter with (nolock)        
      where CategoryName='dimension'         
      and ParamName='Credit Scoring Group'         
      and ParentID is not null) pm         
    ON (CEILING(a.CreditScoreRiil / 10) * 10)=pm.Value2        
    LEFT JOIN DWBIBFI2_DWH..Dim_Credit_Scoring_Group dcs with (nolock) on pm.ParamID_Credit_Scoring_Group=dcs.ID_GroupScoringLevel2) rg         
 on a.ApplicationID = rg.ID_Application  
 --DMD 605  
 LEFT JOIN (SELECT a.ApplicationID, COUNT(a.PaidDate) AS Installment_Paid, MAX(CAST(PaidDate AS DATE)) AS LastPaidDate  
      FROM DWBIBFI2_ODS.dbo.ODS_InstallmentSchedule a WITH (NOLOCK)  
      WHERE InstallmentAmount = PaidAmount  
      GROUP BY a.ApplicationID) lpd  
  ON a.ApplicationID = lpd.ApplicationID  
 LEFT JOIN (SELECT ApplicationID  
       , MAX(CASE WHEN InstallmentAmount = PaidAmount THEN DATEDIFF(day,DueDate,PaidDate)  
         WHEN --PaidAmount > 0 AND 20240205 Remark Comment DMD-605  
         PaidAmount < InstallmentAmount THEN DATEDIFF(day,DueDate,GETDATE()-1)  
         ELSE 0  
       END) AS HighestPassdue  
      FROM DWBIBFI2_ODS.dbo.ODS_InstallmentSchedule WITH (NOLOCK)  
      GROUP BY ApplicationID) hpd  
  ON a.ApplicationID = hpd.ApplicationID  
 LEFT JOIN DWBIBFI2_DWH.dbo.Fact_Contract_LTV ltv WITH (NOLOCK)  
  ON a.ApplicationID = ltv.ID_Application  
 LEFT JOIN DWBIBFI2_DWH.dbo.Dim_LTV_Group dlt WITH (NOLOCK)  
  ON ltv.SK_LTV_Group_Real = dlt.SK_LTV_Group  
 LEFT JOIN DWBIBFI2_DWH.dbo.Dim_PefindoScore ps WITH (NOLOCK)  
  ON fc.SK_PefindoBegining = ps.SK_PefindoResult
 LEFT JOIN DWBIBFI2_DWH.dbo.Dim_PefindoScore pss WITH (NOLOCK)
  ON fc.SK_PefindoBeginning_Spouse = pss.SK_PefindoResult
 LEFT JOIN DWBIBFI2_STG.dbo.STG_PersonalCustomer pc WITH (NOLOCK)  
  ON a.CustomerID = pc.CustomerID  
 LEFT JOIN DWBIBFI2_STG.dbo.STG_CompanyCustomer cc WITH (NOLOCK)  
  ON a.CustomerID = cc.CustomerID  
 --END DMD 605  
  ;        
        
        
update DWBIBFI2_DWH..Cust360_Agreement set isCurrent = 0        
From DWBIBFI2_DWH..Cust360_Agreement a        
Left Join DWBIBFI2_DWH..Cust360_Agreement_Temp b        
 on  isnull(a.CustomerID,'') = isnull(b.CustomerID,'')        
 and isnull(a.ApplicationID,'') = isnull(b.ApplicationID,'')        
 and isnull(a.ContractStatus,'') = isnull(b.ContractStatus,'')        
 and isnull(a.RRDDate,'19000101') = isnull(b.RRDDate,'19000101')        
 and isnull(a.InstallmentAmount,0) = isnull(b.InstallmentAmount,0)        
 and isnull(a.LTV_byTotalOTR,0) = isnull(b.LTV_byTotalOTR,0)        
 and isnull(a.OutstandingPrincipal,0) = isnull(b.OutstandingPrincipal,0)        
 and isnull(a.SK_Product,0) = isnull(b.SK_Product,0) --add by Novan 2021-10-12        
Where b.CustomerID is null  
And a.iscurrent = 1 ;        
        
Insert Into DWBIBFI2_DWH..Cust360_Agreement         
Select a.*        
From DWBIBFI2_DWH..Cust360_Agreement_Temp a        
Left Join DWBIBFI2_DWH..Cust360_Agreement b        
 on  isnull(a.CustomerID,'') = isnull(b.CustomerID,'')        
 and isnull(a.ApplicationID,'') = isnull(b.ApplicationID,'')        
 and isnull(a.ContractStatus,'') = isnull(b.ContractStatus,'')        
 and isnull(a.RRDDate,'19000101') = isnull(b.RRDDate,'19000101')        
 and isnull(a.InstallmentAmount,0) = isnull(b.InstallmentAmount,0)        
 and isnull(a.LTV_byTotalOTR,0) = isnull(b.LTV_byTotalOTR,0)        
 and isnull(a.OutstandingPrincipal,0) = isnull(b.OutstandingPrincipal,0)        
 and isnull(a.SK_Product,0) = isnull(b.SK_Product,0) --add by Novan 2021-10-12        
 and b.isCurrent = 1        
Where b.CustomerID is null ;  


--Fixing DMD 605 20240220
update b
SET b.[LastPaidDate] = a.[LastPaidDate],
	b.[DueDate] = a.[DueDate],
	b.[Installment_Paid] = a.[Installment_Paid],
	b.[HighestPassDue] = a.[HighestPassDue],
	b.[LTV_Real] = a.[LTV_Real],
	b.[PefindoResultDebitur] = a.[PefindoResultDebitur],
	b.[MobilePhone] = a.[MobilePhone],
	b.[PefindoResultPasangan] = a.[PefindoResultPasangan]
From DWBIBFI2_DWH..Cust360_Agreement_Temp a      
Left Join DWBIBFI2_DWH..Cust360_Agreement b      
 on  isnull(a.CustomerID,'') = isnull(b.CustomerID,'')      
 and isnull(a.ApplicationID,'') = isnull(b.ApplicationID,'')      
 and isnull(a.ContractStatus,'') = isnull(b.ContractStatus,'')      
 and isnull(a.RRDDate,'19000101') = isnull(b.RRDDate,'19000101')      
 and isnull(a.InstallmentAmount,0) = isnull(b.InstallmentAmount,0)      
 and isnull(a.LTV_byTotalOTR,0) = isnull(b.LTV_byTotalOTR,0)      
 and isnull(a.OutstandingPrincipal,0) = isnull(b.OutstandingPrincipal,0)      
 and isnull(a.SK_Product,0) = isnull(b.SK_Product,0)
 and b.isCurrent = 1;
--End Fixing DMD 605 20240220
      
       
update UC_DEV..TblStatusJob set SQL_RowCount = b.SQL_RowCount ,RunningStatus = 'S', EndDate = getdate()         
From  UC_DEV..TblStatusJob a        
Inner Join (        
    select 'cust360_agreement' as TableName        
      , count(*) as SQL_RowCount         
    From DWBIBFI2_DWH..cust360_agreement         
    where cast(Process_Date as varchar(8)) = convert(varchar(8),(dateadd(DAY,0,getdate())),112)        
    ) b        
 on a.subJobName = b.TableName        
where subJobName = 'cust360_agreement' ;        
       
       
        
END
