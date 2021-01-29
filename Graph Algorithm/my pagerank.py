import logging
from pyArango.connection import *
import os.path

logging.basicConfig(
    filename='log.log', filemode='a',
    **LOGGING_CONFIG_DICT
)
logger = logging.getLogger(__name__)

today = datetime.today()
today = today.strftime('%Y%m%d_%H:%M:%S')


def log(func):
    logger.info(f'Strart {func.__name__}....')

    def wrapper(*args, **kwargs):
        func(*args, **kwargs)
    logger.info(f'Complete {func.__name__}....')
    return wrapper


class MongoConnector:
    def __init__(self):
        logger.info('Init Mongodb connector...')

    def load_mongo_db(self, collection):
        return (
            spark.read.format("com.mongodb.spark.sql.DefaultSource")
            .option("uri", "mongodb://rms:AbcD5343s@10.60.37.20:27017/risk_management."+collection)
            .load()
        )

    def write_mongo_db(self, df, collection, mode):
        (
            df.write.format("com.mongodb.spark.sql.DefaultSource")
            .mode(mode)
            .option("uri", "mongodb://rms:AbcD5343s@10.60.37.20:27017/risk_management."+collection)
            .save()
        )


class ArangoDBConnector:
    connection: Connection
    db: DBHandle

    def __init__(self, arangoURL: str, username: str, password: str, collection: str):
        self.connection = Connection(
            arangoURL=arangoURL, username=username, password=password)
        self.db = self.connection[collection]
        logger.info('Init ArangoDB connector...')


class Config:

    # documents and edges
    document_userID = 'anhnh6_userid'
    document_deviceID = 'anhnh6_deviceid'
    edge_transfer_money = 'anhnh6_transfer'
    edge_userID_deviceID = 'anhnh6_device_payment'
    # graph
    graph_transfer = 'transfer_graph'
    graph_scoring = 'user_scoring'
    # database
    db_internal_account = 'internal_account'
    db_risk_abuse_score = 'risk_abuse_score_test'
    db_post_mortem = 'post_mortem_result_v1'
    # flow
    isReset = True
    # path
    log_path = './'
    temp_file_path = './'

    def __init__(self):
        pass


class CustomizedPageRank:

    arangodb_connector: ArangoDBConnector
    mongo_connector: MongoConnector
    list_update_user: set

    def __init__(self):
        self.arangodb_connector = ArangoDBConnector(
            arangoURL='http://10.40.38.4:8529', username='kiettt4', password='0l1tDGmBiY9zJ5wUc', collection='user_network')
        self.mongo_connector = MongoConnector()
        self.list_update_user = set()

    @log
    def reset_user(self):
        """[summary]
        """
        reset_query_user_query = '''for u in %s  
            filter u.scoring.abuseScore >0 or u.scoring.avgAbuseScore > 0
            update
            {_key:u._key , 
                scoring: {
                    abuseScore: 0,
                    avgAbuseScore:0,
                    transferScore:0, 
                    transferShareScore:0, 
                    preAddedScoreTransfer:0 , 
                    deviceIDPaymentScore:0, 
                    deviceIDPaymentShareScore:0, 
                    preAddedScoreDevice:0,
                    flagDevice:0,
                    flagTransfer:0
                    }
            }
            in %s OPTIONS { exclusive: true }''' % (Config.document_userID, Config.document_userID)
        self.arangodb_connector.db.AQLQuery(reset_query_user_query)
        return self

    @log
    def reset_device(self):
        """[summary]
        """
        reset_device_query = '''for d in %s  
            filter 
                d.scoring.flag ==1 or d.scoring.abuseScore > 0
            update
                {_key:d._key ,scoring: {abuseScore: 0,
                    flag :0 ,preAddedScore:0, shareScore:0}}
            in %s OPTIONS { exclusive: true }''' % (Config.document_deviceID, Config.document_deviceID)
        self.arangodb_connector.db.AQLQuery(reset_device_query)
        return self

    @log
    def _load_post_mortem(self) -> list:
        """[summary]
        """
        df_post_mortem = self.mongo_connector.load_mongo_db(
            Config.db_post_mortem).drop('_id').where(col('depth') <= 1).select('userID')

        if Config.isReset == True:
            df_new_abusers = df_post_mortem
        else:
            try:
                list_pm_result = glob.glob(os.path.join(
                    Config.temp_file_path, './PM_previous_result*'))
                list_pm_result.sort(reverse=True)

                df_post_mortem_history = spark.createDataFrame(
                    pd.read_parquet(list_pm_result[0]))
            except:
                df_post_mortem_history = spark.createDataFrame(
                    pd.DataFrame([{'userID': 1}]))

            df_new_abusers = df_post_mortem.subtract(df_post_mortem_history)

        # save current result of PM to file
        df_post_mortem.toPandas().to_parquet(os.path.join(Config.temp_file_path,
                                                          'PM_previous_result_{}.parquet'.format(today)))
        logger.info('Saved to PM_previous_result.parquet')

        return [str(x['userID']) for x in df_new_abusers.collect()]

    @log
    def import_label(self):
        """[summary]
        """
        list_abuser = self._load_post_mortem()
        self.list_update_user = set(
            [Config.document_userID+'/'+x for x in list_abuser])

        logger.info('Number of new abuser: ' + str(len(list_abuser)))
        update_label_query = '''FOR user IN @listUserID
                        UPSERT { _key: user } 
                        INSERT {_key: user, 
                                    scoring: {abuseScore: 1, 
                                        transferScore:1, 
                                        transferShareScore:1, 
                                        preAddedScoreTransfer:1, 
                                        deviceIDPaymentScore:1, 
                                        deviceIDPaymentShareScore:1, 
                                        preAddedScoreDevice:1,
                                        flagTransfer:1,
                                        flagDevice:1
                                        
                                        }}
                        UPDATE {_key: user, 
                          scoring: {abuseScore: 1,
                                        transferScore:1, 
                                        transferShareScore:1, 
                                        preAddedScoreTransfer:1, 
                                        deviceIDPaymentScore:1, 
                                        deviceIDPaymentShareScore:1, 
                                        preAddedScoreDevice:1,
                                        flagTransfer:1,
                                        flagDevice:1
                                        }}
                        IN %s OPTIONS { exclusive: true }''' % (Config.document_userID)

        logger.info('Starting import label to ArangoDB')
        batch = 500
        for i in range(0, len(list_abuser), batch):
            bind_var = {'listUserID': list_abuser[i:i+batch]}
            try:
                self.arangodb_connector.db.AQLQuery(
                    update_label_query, bindVars=bind_var)
                logger.info('Import label to ArangoDB: '+str(i))
            except Exception as e:
                logger.info(e, list_abuser[i:i+batch])
        return self

    @log
    def _setup_share_score_user(self):
        setup_share_score_user_query = '''for user in %s
                filter
                    user.isWhiteList != 1 
                    and ( user.scoring.preAddedScoreTransfer > 0.1 
                        or user.scoring.preAddedScoreDevice > 0.1 )
                
                
                let listInboundTransfer = (for e in %s 
                                            filter e._to == user._id
                                            return length(e.transactions))  
                let shareScoreTransfer = user.scoring.preAddedScoreTransfer/ (sum(listInboundTransfer) +1)
                
                let listRelatedDevice = (for e in %s 
                                        filter e._from == user._id
                                        return LENGTH(e.payments))    
                let shareScoreDevice = user.scoring.preAddedScoreDevice / (sum(listRelatedDevice) +1)

                
                update {_key: user._key, 
                        scoring: merge( user.scoring, { transferShareScore: shareScoreTransfer,
                                                        deviceIDPaymentShareScore:shareScoreDevice, 
                                                        preAddedScoreTransfer:0, 
                                                        preAddedScoreDevice:0,
                                                        flagTransfer:shareScoreTransfer >0.001?1:0, 
                                                        flagDevice:shareScoreDevice>0.001?1:0}) } 
                in %s OPTIONS { exclusive: true }
            ''' % (Config.document_userID, Config.edge_transfer_money, Config.edge_userID_deviceID, Config.document_userID)
        self.arangodb_connector.db.AQLQuery(setup_share_score_user_query)
        return self

    @log
    def _setup_share_score_device_payment(self):
        setup_share_score_device_query = \
            '''
        for device in %s
            filter device.scoring.preAddedScore > 0.1
            
            let listRelatedNodes = (for e in %s 
                                    filter e._to == device._id
                                    return LENGTH(e.payments))    
            let shareScore = device.scoring.preAddedScore/ (sum(listRelatedNodes) +1)
            
            update {_key: device._key, scoring: merge(device.scoring, {shareScore: shareScore, flag: shareScore>0.001?1:0, preAddedScore:0})} in %s OPTIONS { exclusive: true }
        ''' % (Config.document_deviceID, Config.edge_userID_deviceID, Config.document_deviceID)
        self.arangodb_connector.db.AQLQuery(setup_share_score_device_query)
        return self

    @log
    def setup_share_score(self):
        self._setup_share_score_user()
        self._setup_share_score_device_payment()
        return self

    @log
    def _get_add_score_user_in_transfer(self):
        adding_score_user_transfer_query = ''' 
            for e in %s 
                let receiver = document(e._to)
                filter receiver.scoring.flagTransfer == 1
                let sender = e._from
                    COLLECT theID = sender
                    AGGREGATE addedScore = sum(receiver.scoring.transferShareScore * length(e.transactions))
                    return {theID, addedScore}
        ''' % (Config.edge_transfer_money)
        queryResult = self.arangodb_connector.db.AQLQuery(
            adding_score_user_transfer_query, batchSize=100, rawResults=True)

        result_add_user_transfer_score = []
        result_add_user_transfer_score += queryResult.response['result']
        while queryResult.response["hasMore"]:
            queryResult.nextBatch()
            result_add_user_transfer_score += queryResult.response['result']
        logger.info('Number user is added score: ' +
                    str(len(result_add_user_transfer_score)))
        return result_add_user_transfer_score

    @log
    def _get_add_score_user_in_device_network(self):
        adding_score_user_device_network_query = \
            '''
                for e in %s 
                filter length(e.payments) >0
                    let device = document(e._to)
                    filter device.scoring.flag == 1
                        COLLECT theID = e._from
                        AGGREGATE addedScore = sum(device.scoring.shareScore* length(e.payments))
                        return {theID, addedScore}
                ''' % (Config.edge_userID_deviceID)
        queryResult = self.arangodb_connector.db.AQLQuery(
            adding_score_user_device_network_query, rawResults=True)
        result_add_user_device_network_score = []
        result_add_user_device_network_score += queryResult.response['result']
        while queryResult.response["hasMore"]:
            queryResult.nextBatch()
            result_add_user_device_network_score += queryResult.response['result']
        logger.info('Number user is added score: ' +
                    str(len(result_add_user_device_network_score)))
        return result_add_user_device_network_score

    @log
    def _get_add_score_device_in_device_network(self):
        adding_score_device_query = \
            '''
            for e in %s 
                filter length(e.payments) >0 
                let user = document(e._from)
                filter user.scoring.flagDevice == 1
            
                    COLLECT theID = e._to
                    AGGREGATE addedScore = sum(user.scoring.deviceIDPaymentShareScore* length(e.payments))
                    return {theID, addedScore}
            ''' % (Config.edge_userID_deviceID)
        queryResult = self.arangodb_connector.db.AQLQuery(
            adding_score_device_query, rawResults=True)
        result_add_device_score = []
        result_add_device_score += queryResult.response['result']
        while queryResult.response["hasMore"]:
            queryResult.nextBatch()
            result_add_device_score += queryResult.response['result']
        logger.info('Number device is added score: ' +
                    str(len(result_add_device_score)))
        return result_add_device_score

    @log
    def _reset_flag_user(self):
        reset_flag_user_query = ''' for u in %s 
                        filter u.scoring.flagTransfer == 1 or u.scoring.flagDevice == 1
                        update {_key: u._key, scoring: merge(u.scoring, {flagTransfer:0,flagDevice:0 }) } in %s OPTIONS { exclusive: true }
                        ''' % (Config.document_userID, Config.document_userID)
        self.arangodb_connector.db.AQLQuery(reset_flag_user_query)

    @log
    def _reset_flag_device(self):
        reset_flag_device_query = ''' for device in %s 
                        filter device.scoring.flag ==1 
                        update {_key: device._key, scoring: merge(device.scoring, {flag: 0})  } in %s OPTIONS { exclusive: true }
                        ''' % (Config.document_deviceID, Config.document_deviceID)
        self.arangodb_connector.db.AQLQuery(reset_flag_device_query)

    @log
    def reset_flag(self):
        self._reset_flag_user()
        self._reset_flag_device()
        return self

    @log
    def add_score_user_transfer(self):
        add_score_user_transfer_query = '''FOR aggEntry IN @aggData
                        LET user = DOCUMENT(aggEntry.theID)
                        FILTER user.isWhiteList !=1
                        UPDATE user WITH 
                        { scoring: merge(user.scoring == null?{}:user.scoring, {transferScore: user.scoring.transferScore + aggEntry.addedScore,
                                                        preAddedScoreTransfer: aggEntry.addedScore }) }
                        IN %s OPTIONS { exclusive: true }''' % (Config.document_userID)

        result_add_user_transfer_score = self._get_add_score_user_in_transfer()
        bind_var = {'aggData': result_add_user_transfer_score}
        self.arangodb_connector.db.AQLQuery(
            add_score_user_transfer_query, bindVars=bind_var)
        
        self.list_update_user.union(set(x['theID'] for x in result_add_user_transfer_score))
        return self

    @log 
    def add_score_user_device_network(self):
        add_score_user_in_device_network_query = '''
                        FOR aggEntry IN @aggData
                        LET user = DOCUMENT(aggEntry.theID)
                        FILTER user.isWhiteList !=1
                        
                        UPDATE user WITH 
                        { scoring: merge(user.scoring ==null?{}:user.scoring,{deviceIDPaymentScore: user.scoring.deviceIDPaymentScore + aggEntry.addedScore,
                                preAddedScoreDevice: aggEntry.addedScore })  }
                        IN %s OPTIONS { exclusive: true }
                        ''' % (Config.document_userID)

        result_add_user_device_network_score = self._get_add_score_user_in_device_network()
        bind_var ={'aggData':result_add_user_device_network_score }
        logger.info('Starting add score for user in device network')
        self.arangodb_connector.db.AQLQuery(add_score_user_in_device_network_query, bindVars=bind_var)
        logger.info('Complete add score for user in device network')
        return self


    @log 
    def add_score_device_in_device_network(self):
        add_score_device_query = '''
                        FOR aggEntry IN @aggData
                        LET device = DOCUMENT(aggEntry.theID)
                        UPDATE device WITH 
                            { scoring: merge( device.scoring == null ?{}:device.scoring, { abuseScore: device.scoring.abuseScore + aggEntry.addedScore,
                                preAddedScore:  aggEntry.addedScore })}
                        IN %s OPTIONS { exclusive: true,ignoreErrors: true  }
                        ''' %(Config.document_deviceID)

        result_add_device_score =  self._get_add_score_device_in_device_network()
        bind_var ={'aggData':result_add_device_score }
        logger.info('Starting add score for device')
        self.arangodb_connector.db.AQLQuery(add_score_device_query, bindVars=bind_var)
        logger.info('Complete add score for device')
        return self
