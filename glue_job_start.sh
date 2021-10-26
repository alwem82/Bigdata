aws glue start-job-run --job-name 'MtoS3 PiiList' &
aws glue start-job-run --job-name 'MtoS3 ApplicationTrain' &
aws glue start-job-run --job-name 'MtoS3 Bureau' &
aws glue start-job-run --job-name 'MtoS3 BureauBalance' &
aws glue start-job-run --job-name 'MtoS3 CreditCardBalance' &
aws glue start-job-run --job-name 'MtoS3 InstallmentsPayments' &
aws glue start-job-run --job-name 'MtoS3 PosCashBalance' &
aws glue start-job-run --job-name 'MtoS3 PreApplication' &