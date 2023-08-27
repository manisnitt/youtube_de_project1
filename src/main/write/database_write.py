from src.main.utility.logging_config import *

class DatabaseWriter:
    def __init__(self,url,properties):
        self.url = url
        self.properties = properties

    def write_dataframe(self,df,table_name):
        try:
            print("inside write_dataframe")
            df.write.jdbc(url=self.url,
                          table=table_name,
                          mode="append",
                          properties=self.properties)
            logger.info(f"Data successfully written into {table_name} table ")
        except Exception as e:
            return {f"Message: Error occured {e}"}
