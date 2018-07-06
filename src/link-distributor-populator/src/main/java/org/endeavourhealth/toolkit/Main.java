package org.endeavourhealth.toolkit;

import OpenPseudonymiser.Crypto;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.zaxxer.hikari.HikariDataSource;
import org.endeavourhealth.common.cache.ObjectMapperPool;
import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.admin.LinkDistributorPopulatorDalI;
import org.endeavourhealth.core.database.dal.admin.LinkDistributorTaskListDalI;
import org.endeavourhealth.core.database.dal.admin.models.LinkDistributorTaskList;
import org.endeavourhealth.core.database.dal.subscriberTransform.EnterpriseIdDalI;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.json.ConfigParameter;
import org.endeavourhealth.json.LinkDistributorConfig;
import org.endeavourhealth.json.LinkDistributorModel;
import org.endeavourhealth.json.PatientPseudoDetails;
import org.hibernate.internal.SessionImpl;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private static final String PSEUDO_KEY_NHS_NUMBER = "NHSNumber";
    private static final String PSEUDO_KEY_PATIENT_NUMBER = "PatientNumber";
    private static final String PSEUDO_KEY_DATE_OF_BIRTH = "DOB";

    private static LinkDistributorTaskListDalI taskRepository = DalProvider.factoryLinkDistributorTaskListDal();
    private static LinkDistributorPopulatorDalI populatorRepository = DalProvider.factoryLinkDistributorPopulatorDal();
    private static Map<String, List<LinkDistributorConfig>> linkDistributorCacheMap = new HashMap<>();

    private static Map<String, HikariDataSource> connectionPools = new ConcurrentHashMap<>();
    private static Map<String, byte[]> saltCacheMap = new HashMap<>();
    private static Map<String, String> escapeCharacters = new ConcurrentHashMap<>();

    private static String insertSQL = "INSERT INTO link_distributor (source_skid, target_salt_key_name, target_skid ) " +
            " VALUES (?, ?, ?)" +
            " ON DUPLICATE KEY UPDATE " +
            " target_skid = VALUES(target_skid);";

    /**
     * utility to update the Age columns in Enterprise
     *
     * Usage
     * =================================================================================
     * No parameters
     */
    public static void main(String[] args) throws Exception {

        try {
            LOG.info("Checking if it is safe to run");

            if (!taskRepository.safeToRun()) {
                LOG.info("Not safe to run...exiting");
                return;
            }

            LinkDistributorTaskList task = taskRepository.getNextTaskToProcess();

            if (task == null) {
                LOG.info("Nothing to do...exiting");
                return;
            }

            try {
                initialiseTask(task);
                processPatientsInBatches(task.getConfigName(), 1000);
                taskRepository.updateTaskStatus(task.getConfigName(), (byte)2);

            } catch (Exception e) {
                taskRepository.updateTaskStatus(task.getConfigName(), (byte)3);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    private static void initialiseTask(LinkDistributorTaskList task) throws Exception {

        LOG.info("Updating status to 'processing'");
        taskRepository.updateTaskStatus(task.getConfigName(), (byte)1);

        LOG.info("Clearing down the populator table");
        populatorRepository.clearDown();

        LOG.info("Populating the populator table");
        populatorRepository.populate();

    }

    private static void processPatientsInBatches(String configName, Integer batchSize) throws Exception {
        LOG.info("Populating link distributor table from link_distributor_populator");

        try {

            boolean stopProcessing = false;

            List<PatientPseudoDetails> patients = new ArrayList<>();

            while (!stopProcessing) {
                patients.clear();

                LOG.info("Getting next batch of patients");
                patients = getNextBatchOfPatientsToProcess(batchSize);

                if (patients.size() < 1) {
                    stopProcessing = true;
                }

                bulkProcessLinkDistributor(patients, configName);
            }

        } catch (Throwable t) {
            LOG.error("", t);
        }
    }

    private static List<PatientPseudoDetails> getNextBatchOfPatientsToProcess(Integer batchSize) throws Exception {
        EntityManager entityManager = ConnectionManager.getAdminEntityManager();
        SessionImpl session = (SessionImpl) entityManager.getDelegate();
        Connection connection = session.connection();
        Statement statement = connection.createStatement();

        List<PatientPseudoDetails> patients = new ArrayList<>();

        String sql = String.format("SELECT patient_id, nhs_number, date_of_birth FROM link_distributor_populator WHERE done = 0 limit %d",
                batchSize);

        try {
            ResultSet rs = statement.executeQuery(sql);
            while (rs.next()) {
                PatientPseudoDetails pat = new PatientPseudoDetails();
                pat.setPatientId(rs.getString(1));
                pat.setNhsNumber(rs.getString(2));
                pat.setDateOfBirth(rs.getDate(3));
                patients.add(pat);
            }
            rs.close();
        } catch (Throwable t) {
            LOG.error("", t);
        }
        finally {
            statement.close();
            entityManager.close();
        }

        return patients;

    }

    private static void bulkProcessLinkDistributor(List<PatientPseudoDetails> patients, String configName) throws Exception {

        LOG.info("Processing Batch of patients");
        List<LinkDistributorModel> processedPatients = new ArrayList<>();
        List<LinkDistributorConfig> linkDistributorConfigs = getLinkedDistributorConfig(configName);
        if (linkDistributorConfigs != null) {
            for (PatientPseudoDetails patient : patients) {
                checkPatientHasBeenSentToSubscriber(configName, patient.getPatientId());
                // obtain the source pseudo Id
                String sourceSkid = pseudonymiseFromPatientDetails(patient, configName);
                if (sourceSkid == null) {
                    continue;
                }

                for (LinkDistributorConfig ldConfig : linkDistributorConfigs) {
                    LinkDistributorModel model = new LinkDistributorModel();
                    model.setSourceSkid(sourceSkid);
                    model.setTargetSalkKeyName(ldConfig.getSaltKeyName());
                    model.setTargetSkid(pseudonymiseUsingConfigFromPatientDetails(patient, ldConfig));

                    processedPatients.add(model);
                }
            }
        }

        saveLinkDistributorData(processedPatients, configName);
        updateDoneFlag(patients);
    }

    private static void saveLinkDistributorData(List<LinkDistributorModel> patients, String configName) throws Exception {
        LOG.info("Saving Now");
        JsonNode config = ConfigManager.getConfigurationAsJson(configName, "db_subscriber");
        String url = config.get("enterprise_url").asText();
        Connection connection = openConnection(url, config);
        connection.setAutoCommit(true);

        PreparedStatement insert = connection.prepareStatement(insertSQL);

        for (LinkDistributorModel pat : patients) {
            insert.setString(1, pat.getSourceSkid());
            insert.setString(2, pat.getTargetSalkKeyName());
            insert.setString(3, pat.getTargetSkid());

            insert.addBatch();
        }

        try {
            LOG.info("Filing");
            insert.executeBatch();
            insert.clearBatch();
            LOG.info("Done Insert");
        } catch (Exception ex) {
            LOG.error(insert.toString());
            throw ex;
        } finally {
            connection.close();
        }
    }private static Connection openConnection(String url, JsonNode config) throws Exception {

        HikariDataSource pool = connectionPools.get(url);
        if (pool == null) {
            synchronized (connectionPools) {
                pool = connectionPools.get(url);
                if (pool == null) {

                    String driverClass = config.get("driverClass").asText();
                    String username = config.get("enterprise_username").asText();
                    String password = config.get("enterprise_password").asText();

                    //force the driver to be loaded
                    Class.forName(driverClass);

                    pool = new HikariDataSource();
                    pool.setJdbcUrl(url);
                    pool.setUsername(username);
                    pool.setPassword(password);
                    pool.setMaximumPoolSize(3);
                    pool.setMinimumIdle(1);
                    pool.setIdleTimeout(60000);
                    pool.setPoolName("EnterpriseFilerConnectionPool" + url);
                    pool.setAutoCommit(false);

                    connectionPools.put(url, pool);

                    //cache the escape string too, since getting the metadata each time is extra load
                    Connection conn = pool.getConnection();
                    String escapeStr = conn.getMetaData().getIdentifierQuoteString();
                    escapeCharacters.put(url, escapeStr);
                    conn.close();
                }
            }
        }

        return pool.getConnection();
    }

    private static void updateDoneFlag(List<PatientPseudoDetails> patients) throws Exception {

        List<String> patientIds = patients.stream().map(PatientPseudoDetails::getPatientId).collect(Collectors.toList());

        populatorRepository.updateDoneFlag(patientIds);

    }

    private static String pseudonymiseFromPatientDetails(PatientPseudoDetails patient, String configName) throws Exception {
        String dob = null;
        if (patient.getDateOfBirth() != null) {
            Date d = patient.getDateOfBirth();
            dob = new SimpleDateFormat("dd-MM-yyyy").format(d);
        }

        if (Strings.isNullOrEmpty(dob)) {
            //we always need DoB for the psuedo ID
            return null;
        }

        TreeMap<String, String> keys = new TreeMap<>();
        keys.put(PSEUDO_KEY_DATE_OF_BIRTH, dob);

        String nhsNumber = patient.getNhsNumber();
        if (!Strings.isNullOrEmpty(nhsNumber)) {
            keys.put(PSEUDO_KEY_NHS_NUMBER, nhsNumber);

        } else {
            return null;
        }

        return applySaltToKeys(keys, getEncryptedSalt(configName));
    }

    private static byte[] getEncryptedSalt(String configName) throws Exception {

        byte[] ret = saltCacheMap.get(configName);
        if (ret == null) {

            synchronized (saltCacheMap) {
                ret = saltCacheMap.get(configName);
                if (ret == null) {

                    JsonNode config = ConfigManager.getConfigurationAsJson(configName, "db_subscriber");
                    JsonNode saltNode = config.get("salt");
                    if (saltNode == null) {
                        throw new Exception("No 'Salt' element found in Enterprise config " + configName);
                    }
                    String base64Salt = saltNode.asText();

                    ret = Base64.getDecoder().decode(base64Salt);
                    saltCacheMap.put(configName, ret);
                }
            }
        }
        return ret;
    }

    private static String pseudonymiseUsingConfigFromPatientDetails(PatientPseudoDetails patient, LinkDistributorConfig config) throws Exception {
        TreeMap<String, String> keys = new TreeMap<>();

        List<ConfigParameter> parameters = config.getParameters();

        for (ConfigParameter param : parameters) {
            String fieldValue = null;
            if (param.getFieldName().equals("date_of_birth")) {
                if (patient.getDateOfBirth() !=null) {
                    Date d = patient.getDateOfBirth();
                    fieldValue = new SimpleDateFormat("dd-MM-yyyy").format(d);
                }
            }

            if (param.getFieldName().equals("nhs_number")) {
                fieldValue = patient.getNhsNumber();
            }

            if (Strings.isNullOrEmpty(fieldValue)) {
                // we always need a non null string for the psuedo ID
                return null;
            }

            keys.put(param.getFieldLabel(), fieldValue);
        }

        return applySaltToKeys(keys, Base64.getDecoder().decode(config.getSalt()));
    }

    private static String applySaltToKeys(TreeMap<String, String> keys, byte[] salt) throws Exception {
        Crypto crypto = new Crypto();
        crypto.SetEncryptedSalt(salt);
        return crypto.GetDigest(keys);
    }

    private static List<LinkDistributorConfig> getLinkedDistributorConfig(String configName) throws Exception {

        List<LinkDistributorConfig> ret = linkDistributorCacheMap.get(configName);
        if (ret == null) {
            synchronized (linkDistributorCacheMap) {
                ret = linkDistributorCacheMap.get(configName);
                if (ret == null) {

                    ret = new ArrayList<>();

                    JsonNode config = ConfigManager.getConfigurationAsJson(configName, "db_subscriber");
                    JsonNode linkDistributorsNode = config.get("linkedDistributors");

                    if (linkDistributorsNode == null) {
                        return null;
                    }
                    String linkDistributors = convertJsonNodeToString(linkDistributorsNode);
                    LinkDistributorConfig[] arr = ObjectMapperPool.getInstance().readValue(linkDistributors, LinkDistributorConfig[].class);

                    for (LinkDistributorConfig l : arr) {
                        ret.add(l);
                    }

                    if (ret == null) {
                        return null;
                    }
                    linkDistributorCacheMap.put(configName, ret);
                }
            }
        }
        return ret;
    }

    private static String convertJsonNodeToString(JsonNode jsonNode) throws Exception {
        try {
            ObjectMapper mapper = new ObjectMapper();
            Object json = mapper.readValue(jsonNode.toString(), Object.class);
            return mapper.writeValueAsString(json);
        } catch (Exception e) {
            throw new Exception("Error parsing Link Distributor Config");
        }
    }

    private static boolean checkPatientHasBeenSentToSubscriber(String configName, String patientId) throws Exception {
        EnterpriseIdDalI dal = DalProvider.factoryEnterpriseIdDal(configName);
        Long enterprisePatientId = dal.findEnterpriseId(ResourceType.Patient.toString(), patientId);
        if (enterprisePatientId != null) {
            return true;
        }

        return false;
    }

    private static String getKeywordEscapeChar(String url) {
        return escapeCharacters.get(url);
    }

}
