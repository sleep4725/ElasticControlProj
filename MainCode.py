from EsCode.esController import EsController

""" 테스트 환경   : window 10
    테스트 개발툴 : PyCharm
    Python interpreter version : 3.6.1
    주의 : Elasticsearch module install 필요
           Elasticsearch service 구동 필요
           
    최초 구동시 : ./Config/esConfig.yml 파일 수정 필요 
"""
def main():
    o = EsController()
    o.scroll_search()

if __name__ == "__main__":
    main()