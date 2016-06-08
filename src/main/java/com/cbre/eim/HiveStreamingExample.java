package com.cbre.eim;

import java.io.File;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.streaming.ConnectionError;
import org.apache.hive.hcatalog.streaming.DelimitedInputWriter;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.hcatalog.streaming.ImpersonationFailed;
import org.apache.hive.hcatalog.streaming.InvalidColumn;
import org.apache.hive.hcatalog.streaming.InvalidPartition;
import org.apache.hive.hcatalog.streaming.InvalidTable;
import org.apache.hive.hcatalog.streaming.PartitionCreationFailed;
import org.apache.hive.hcatalog.streaming.StreamingConnection;
import org.apache.hive.hcatalog.streaming.StreamingException;
import org.apache.hive.hcatalog.streaming.TransactionBatch;

public class HiveStreamingExample {

    private static String dbName = "streamsets";
    private static String tblName = "sf_account_streaming";
    private static final ArrayList<String> partitionVals = new ArrayList<>(1);

    private static final String serdeClass = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
    private static final Logger LOG = Logger.getLogger(HiveStreamingExample.class.getName());

    private static HiveConf hiveConf;

    public static void main(String[] args) {
        try {
            partitionVals.add("createddate");

            String[] fieldNames = {
                    "id","isdeleted","masterrecordid","name","type","recordtypeid","parentid",
                    "billingstreet","billingcity","billingstate","billingpostalcode","billingcountry","billingstatecode",
                    "billingcountrycode","billinglatitude","billinglongitude","shippingstreet","shippingcity","shippingstate",
                    "shippingpostalcode","shippingcountry","shippingstatecode","shippingcountrycode","shippinglatitude","shippinglongitude",
                    "phone","fax","accountnumber","website","photourl","sic","industry","annualrevenue","numberofemployees",
                    "tickersymbol","description","rating","site","currencyisocode","ownerid","createdbyid","lastmodifieddate",
                    "lastmodifiedbyid","systemmodstamp","lastactivitydate","lastvieweddate","lastreferenceddate","jigsaw","jigsawcompanyid",
                    "accountsource","sicdesc","family_id_formula__c","family_id__c","invalid__c","legacy_account_id__c","legacy_created_date__c",
                    "legacy_edit_date__c","source_broker__c","dig_review_comments__c","iseditmode__c","unique_account_id__c","investor_profile__c",
                    "lender_type__c","source_system__c","d_u_n_s__c","domestic_ultimate__c","exclude_from_dnb__c","global_ultimate__c","hierarchy_code__c",
                    "last_reviewed_by__c","last_reviewed_date__c","legal_status__c","marketability__c","naics_code_description__c","naics_code__c",
                    "number_of_family_members__c","original_account_name__c","parent_account__c","previous_d_u_n_s__c","sic_code_1_description__c",
                    "sic_code_1__c","sic_code_2_description__c","sic_code_2__c","sic_code_3_description__c","sic_code_3__c","sic_code_4_description__c",
                    "sic_code_4__c","sic_code_5_description__c","sic_code_5__c","sic_code_6_description__c","sic_code_6__c","tradestyle_name__c",
                    "opportunities_count__c","client_type__c","key_client_account_for__c","key_pursuit_account_for__c","scanbizcards__scanbizcards__c",
                    "d_b_shell_account__c","insitucah__ultimate_parent_flag__c","insitucah__ultimate_parent__c","geographical_role__c",
                    "account_opportunity_history__c","converted_lead_main_phone__c","of_private_tags__c","account_creator_country__c",
                    "australian_business_number_abn__c","local_account_name__c","local_account_site__c","local_billing_city__c","local_billing_country__c",
                    "local_billing_state_province__c","local_billing_street__c","local_billing_zip_postal_code__c","local_shipping_city__c",
                    "local_shipping_country__c","local_shipping_state_province__c","local_shipping_street__c","local_shipping_zip_postal_code__c",
                    "migrated_account__c","non_utf_8_compliant__c","translation_status__c"
            };

            /////////////////////////////////////////////////////////////////////
            // Build hive configuration
            /////////////////////////////////////////////////////////////////////
            hiveConf = new HiveConf();
            String hiveConfDir = null;
            if(args.length > 1){
                hiveConfDir = args[1];
            }
            else{
                hiveConfDir = "/etc/hive/";
            }

            File hiveConfDirFile = new File(hiveConfDir);

            if (!hiveConfDirFile.isAbsolute()) {
                hiveConfDirFile = new File(System.getProperty("user.dir"), hiveConfDir).getAbsoluteFile();
            }

            File coreSite = new File(hiveConfDirFile.getAbsolutePath(), "core-site.xml");
            File hiveSite = new File(hiveConfDirFile.getAbsolutePath(), "hive-site.xml");
            File hdfsSite = new File(hiveConfDirFile.getAbsolutePath(), "hdfs-site.xml");

            LOG.info(String.format("Using config files: \n\t%s\n\t%s\n\t%s", coreSite.getAbsolutePath(),
                    hiveSite.getAbsolutePath(), hdfsSite.getAbsolutePath()));

            hiveConf.addResource(new Path(coreSite.getAbsolutePath()));
            hiveConf.addResource(new Path(hiveSite.getAbsolutePath()));
            hiveConf.addResource(new Path(hdfsSite.getAbsolutePath()));

            hiveConf.set("fs.azure.account.key.elzdatalakecbre.blob.core.windows.net", "MIIB/wYJKoZIhvcNAQcDoIIB8DCCAewCAQAxggFfMIIBWwIBADBDMC8xLTArBgNVBAMTJEhESW5zaWdodC5Qcm9kdWN0aW9uLkVuY3J5cHRpb24uQ2VydAIQ27YspA7T3KtBzMHdQtljujANBgkqhkiG9w0BAQEFAASCAQCH0Ou215k/3cTy0dDbsoO9VUiV+GbusvRzYmVqpx+GrGYg5zhVOck91eChwT9fuC5CzgG3yBYaVJxlJI9KUzIIb+URZwigKQ+nE9G6JDreoYbg1rBQAqRnREDucHwD1JpHQ15cpWkDKGpilJzXvBlVu1IJwwmMpvRhFwUgZv8w11lmN9y/XbpUuV2azI/j3ZsSxWngb9ksB2imHOQuNgJJktAUS8jfVR0bsjnyvda0tPTQTd3AOVIhrIY87NsxI6LJKAuOnSSyjv7vhHw+ZGEv05JwKyAW6MwDysGRMaEW9334b2E+B8/wHEcMfgbwJ65SsNBhi6FM8nW/vJ2yir9oMIGDBgkqhkiG9w0BBwEwFAYIKoZIhvcNAwcECNaR5yvaHKAUgGAXartALJzQ4ukIyANALVoCDtJiMeYHDpMPNu2XKwdoyPF+kWPTBQ2QE17LlBgkXZk2T95Tu8PKxKvyjbX4eHIXIU/m0yUEUcEsIuZR/OWOP3MKbm1uBNCIBbEtIpWhkLo=");

            LOG.info(String.format("Hive configuration properties"));

            Properties hiveProps = hiveConf.getAllProperties();
            Set<String> propertyNames = hiveProps.stringPropertyNames();
            for(String prop : propertyNames){
                LOG.info(String.format("\t%s: %s", prop, hiveProps.getProperty(prop)));
            }

            HiveEndPoint hiveEP = new HiveEndPoint("thrift://hn0-cbre-d.dtzffnsqd2xerbc2tcalupxush.gx.internal.cloudapp.net:9083", dbName, tblName, null);
            // Auto create partitions and use the Hive configuration we created above
            StreamingConnection connection = hiveEP.newConnection(true, hiveConf);
            DelimitedInputWriter writer
                    = new DelimitedInputWriter(fieldNames, ",", hiveEP);
            TransactionBatch txnBatch = connection.fetchTransactionBatch(10, writer);

            LOG.info(txnBatch.getCurrentTransactionState().toString());
            LOG.info(String.format("%d", txnBatch.getCurrentTxnId()));
            LOG.info("begin transaction");
            txnBatch.beginNextTransaction();
            txnBatch.write("001i000000SABxmAAH,0,,\"Environmental Resources Management - North Central, Inc.\",Master,,,1701 Golf Rd Ste 1-700,Rolling Meadows,Illinois,60008-4249,United States,IL,US,,,,,,,,,,,,+1 (610) 524-3500,,,,/services/images/photo/001i000000SABxmAAH,,,,3500,,,,\"1701 Golf Rd Ste 1-700, Rolling Meadows, United States\",USD,005i0000001qeqWAAQ,10/21/2013 17:37,005i0000001qV8IAAU,4/8/2016 0:28,005i0000006RSWfAAO,4/8/2016 0:28,,,,,,,,001i000001fmAYk,001i000000SABxm,0,Notes_Added-401,,,Data Migration,,,001i000000SABxmAAH,,,,788102213,,0,001i000001fmAYkAAM,7,005i0000003rC4EAAU,7/10/2015 17:18,Corporation,Marketable,Environmental Consulting Services,541620,169,Environmental Resources Management,001i000001VUu4QAAT,21690389,\"Business Consulting Services, Not Elsewhere Classified\",8748,,,,,,,,,,,E R M,0,,,,0,0,,,,,,,,,,,,,,,,,,,,,,,".getBytes());
            txnBatch.write("001i000000SABxnAAH,0,,Epilepsy Foundation Of Greater Chicago,Master,,,17 N State St Fl 6,Chicago,Illinois,60602-3047,United States,IL,US,,,,,,,,,,,,+1 (312) 939-8622,,,,/services/images/photo/001i000000SABxnAAH,,,4555328,15,,,,\"17 N State St Fl 6, Chicago, United States\",USD,005i0000001qeqWAAQ,10/21/2013 17:37,005i0000001qV8IAAU,4/8/2016 0:28,005i0000006RSWfAAO,4/8/2016 0:28,,,,,,,,001i000000SABxn,001i000000SABxn,0,Notes_Added-402,,,Data Migration,,,001i000000SABxnAAH,,,,614728731,,0,,0,005i0000003rC4EAAU,7/10/2015 15:29,Corporation,Marketable,Other Individual and Family Services,624190,0,Epilepsy Foundation,,607274701,Individual and Family Social Services,8322,,,,,,,,,,,,2,Occupier,,,0,0,,,,,,,,,,,,,,,,,,,,,,,".getBytes());
            txnBatch.commit();
            LOG.info("end transaction");
            LOG.info(txnBatch.getCurrentTransactionState().toString());
            LOG.info(String.format("%d", txnBatch.getCurrentTxnId()));
            txnBatch.close();

        } catch (ConnectionError ex) {
            LOG.log(Level.SEVERE, null, ex);
        } catch (InvalidPartition ex) {
            LOG.log(Level.SEVERE, null, ex);
        } catch (InvalidTable ex) {
            LOG.log(Level.SEVERE, null, ex);
        } catch (PartitionCreationFailed ex) {
            LOG.log(Level.SEVERE, null, ex);
        } catch (ImpersonationFailed ex) {
            LOG.log(Level.SEVERE, null, ex);
        } catch (InterruptedException | ClassNotFoundException ex) {
            LOG.log(Level.SEVERE, null, ex);
        } catch (InvalidColumn ex) {
            LOG.log(Level.SEVERE, null, ex);
        } catch (StreamingException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
    }
}