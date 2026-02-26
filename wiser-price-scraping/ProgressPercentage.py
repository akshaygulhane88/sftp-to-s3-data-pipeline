def upload_large_file():
    config = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10,
                            multipart_chunksize=1024 * 25, use_threads=True)
    file_path = os.path.dirname(__file__) + '/wiser.csv'
    key_path = 'multipart_files/wiser.csv'
    s3_resource().meta.client.upload_file(file_path, BUCKET_NAME, key_path,
                                          ExtraArgs={'***': '****', 'ContentType': 'csv'}, --@chitra be careful defning this
                                          Config=config,
                                          Callback=ProgressPercentage(file_path))


class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size, percentage
                )
            )
            sys.stdout.flush()