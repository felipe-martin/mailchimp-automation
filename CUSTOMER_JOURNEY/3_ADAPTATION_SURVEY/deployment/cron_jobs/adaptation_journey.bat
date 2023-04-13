@echo off
echo "[INFO] /////////////// RUNNING ADAPTATION CUSTOMER JOURNEY //////////////"
timeout 1 > NUL
call C:\Users\LMS\Anaconda3\Scripts\activate mailchimp
timeout 5 > NUL
cd C:\Users\LMS\Documents\MAILCHIMP_AUTOMATION\CUSTOMER_JOURNEY\3_ADAPTATION_SURVEY\deployment\
timeout 1 > NUL
python adaptation.py --campaing_email_code ENADAP 