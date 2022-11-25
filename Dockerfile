FROM public.ecr.aws/lambda/python:3.8

# モジュールが必要な場合はrequirements.txtを書いたり、pip installを書いたりします。
COPY container_lambda/requirements.txt ${LAMBDA_TASK_ROOT}

RUN pip install -r requirements.txt

COPY container_lambda/ ${LAMBDA_TASK_ROOT}
COPY rule_base.xlsx ${LAMBDA_TASK_ROOT}
COPY trigger_table.xlsm ${LAMBDA_TASK_ROOT}

# ファイル名と関数名をデフォルトのENTRYPOINTに渡します。gunicornと似てますね。
CMD [ "app.handler" ]

# 注意: volumeをマウントする前提で使用することはできません