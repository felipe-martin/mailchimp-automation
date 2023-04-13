@echo off
echo "[INFO] /////////////// INSERTING ROWS FROM SQL SERVER VITAMINA TO XANO POSTGRES //////////////"
timeout 1 > NUL
call C:\Users\LMS\Anaconda3\Scripts\activate mailchimp
timeout 5 > NUL
cd C:\Users\LMS\Documents\MAILCHIMP_AUTOMATION\CUSTOMER_JOURNEY\1_WELCOME\deployment\
timeout 1 > NUL
python welcome.py --campaing_email_code WELCOME 