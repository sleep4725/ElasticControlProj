import os
import yaml
import requests
from elasticsearch import Elasticsearch

#
# __author__ : KimJunHyeon
# __date__   : 202008
#

class EsClusterGet:

    # 상수
    ES_CONFIG_PATH = "./Config/esConfig.yml"


    """ elastic client node 를 return
        1] return 시 server가 alive 인지 check
        2] cluster의 health check
    """
    @classmethod
    def ret_es_cluster(cls, es_info):
        """
        :param es_info:
        :return:
        """
        response = None
        sess = requests.Session()

        try:
            response = sess.get(url=es_info["esHosts"][0])
        except requests.exceptions.ConnectionError as err:
            print(err)
            exit(1)
        except requests.exceptions.InvalidURL as err:
            print(err)
            exit(1)

        sess.close()
        if response.status_code == 200 and response.ok:
            es = Elasticsearch(hosts=es_info["esHosts"])
            es_status = es.cluster.health()["status"]
            if es_status == "yellow" or es_status == "green":
                return es
            else:
                print("elasticsearch health red !!!")
                exit(1)
        else:
            print("elasticsearch service is close !!!")
            exit(1)


    """ elasticsearch connection 하기 위한 파일 정보 get
    """
    @classmethod
    def get_es_information(cls):
        """
        :return:
        """
        result = os.path.isfile(EsClusterGet.ES_CONFIG_PATH)
        if result:
            with open(EsClusterGet.ES_CONFIG_PATH, "r", encoding="utf-8") as fr:
                es_get_info = yaml.safe_load(fr)
                fr.close()

            return es_get_info
        else:
            print("file({}) not exists !!".format(EsClusterGet.ES_CONFIG_PATH))
            exit(1)