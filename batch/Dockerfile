# Build context must be the repository root:
#   docker build -f batch/Dockerfile -t batch .

FROM public.ecr.aws/lambda/python:3.13

# memory パッケージをインストール
COPY memory/ /tmp/memory/
RUN pip install /tmp/memory/ && rm -rf /tmp/memory/

# batch 追加依存をインストール
RUN pip install python-dotenv

# batch ハンドラファイルをコピー
COPY batch/handler.py batch/consolidation.py batch/mental_model_trigger.py ${LAMBDA_TASK_ROOT}/

CMD ["handler.handler"]
