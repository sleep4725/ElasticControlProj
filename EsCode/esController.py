import os
import json
from elasticsearch import helpers

from EsCode.esClusterGet import EsClusterGet

#
# __author__ : KimJunHyeon
# __date__: 202008
#

class EsController:


    FETCH_COUNT = 200

    def __init__(self):
        self.es_config_info = EsClusterGet.get_es_information()
        self.es_client = EsClusterGet.ret_es_cluster(es_info=self.es_config_info)

    # 검색 ( match_all query )
    def full_text_search(self, index="sample_index"):
        """
        :return:
        """
        body = { "size": 20, "query": {"match_all":{}} }

        """ 조회시 해당 index가 있는지 먼저 확인
        """
        response = self.es_client.indices.exists(index=index)
        if response:
            result = self.es_client.search(index=index, body=body)
            hits = result["hits"]["hits"]
            if len(hits) != 0:
                for h in hits:
                    print(h["_source"])
            else:
                """ index의 데이터가 비어있는 경우 
                """
                print("index is empty !!")
        else:
            """ elasticsearch 에 index가 존재하지 않는 경우 
            """
            print("index is not exists !!")

    # 검색 (pagination 처리)
    def scroll_search(self, index="sample_index"):
        """
        :param index:
        :return:
        """

        """ 조회시 해당 index가 있는지 먼저 확인
        """
        response = self.es_client.indices.exists(index=index)
        if response:
            body = { "size": 20, "query": {"match_all":{}} }
            result = self.es_client.search(
                index= index,
                scroll = "2m",
                size   = EsController.FETCH_COUNT,
                body   = body
            )

            sid = result["_scroll_id"]
            fetched = len(result["hits"]["hits"])

            while fetched > 0:
                """ Scrolling ...
                """
                hits = result["hits"]["hits"]
                for i in range(fetched):
                    print(hits[i]["_source"])

                result = self.es_client.scroll(scroll_id=sid, scroll="2m")
                sid = result["_scroll_id"]
                fetched = len(result["hits"]["hits"])

    # 데이터 삽입 테스트
    def json_file_insert(self):
        """
        :return:
        """
        sample_json_file_path = "../TestFile/sample_data.json"
        result = os.path.isfile(sample_json_file_path)

        if result:
            with open(sample_json_file_path, "r", encoding="utf-8") as fr:
                json_data = json.load(fr)
                fr.close()

                result = [ {"category": k, "value": v} for k, v in dict(json_data).items() ]
                for r in result:
                    self.es_client.index(index="sample_index", body=r)
        else:
            print("file is not exists !!")

    # bulk 로 데이터 insert
    def bulk_document_insert(self):
        """
        :return:
        """
        sample_json_file_path = "../TestFile/sample_data.json"
        result = os.path.isfile(sample_json_file_path)

        if result:
            with open(sample_json_file_path, "r", encoding="utf-8") as fr:
                json_data = json.load(fr)
                fr.close()

                result = [
                    {
                        "_index"  : "sample_index_2",
                        "_source" : {
                            "category": k,
                            "value": v
                        }
                    } for k, v in dict(json_data).items()
                ]

                helpers.bulk(client=self.es_client, actions=result)
        else:
            print("file is not exists !!")

    # 소멸자
    def __del__(self):
        
        try:
            self.es_client.close()
        except AttributeError as err:
            print(err)

