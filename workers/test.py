import os
import sys
sys.path.append('..')


def check_for_client_config(client_id):
    print(f'config/client{client_id}.ini')
    if os.path.exists(f'../config/client{client_id}.ini'):
        return True
    return False


print(check_for_client_config('01'))
