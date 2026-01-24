class ContentSaver:
    def __init__(self, config=None):
        self.config = config

    def to_local(self, content: str, path: str):
        """存储到本地磁盘"""
        pass

    def to_azure(self, content: str, blob_name: str, container: str):
        """存储到 Azure Blob Storage"""
        pass

    def to_aws(self, content: str, s_key: str, bucket: str):
        """存储到 AWS S3"""
        pass