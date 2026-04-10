# Enable OpenSSL legacy provider for ripemd160 (required for Aerospike key digest)
import os
_openssl_conf = '/tmp/openssl-legacy.cnf'
if not os.path.exists(_openssl_conf):
    with open(_openssl_conf, 'w') as f:
        f.write('openssl_conf = openssl_init\n[openssl_init]\nproviders = provider_sect\n'
                '[provider_sect]\ndefault = default_sect\nlegacy = legacy_sect\n'
                '[default_sect]\nactivate = 1\n[legacy_sect]\nactivate = 1\n')
if not os.environ.get('OPENSSL_CONF'):
    os.environ['OPENSSL_CONF'] = _openssl_conf

# from asclient.commands import command
from asclient.client import Client
from asclient.admin import Admin
from asclient.stringops import StringOperations
import time


# Modify the module's variables directly
# asclient.commands.VERBOSE = True
# asclient.commands.VERBOSE1 = True
# asclient.commands.VERBOSE2 = True
# asclient.commands.DEBUG_BUFFER = True
# asclient.commands.VERBOSE4 = True

def rw_client():
    client = Client("localhost", 3000)
    client.connect('admin', 'admin')
    admin = Admin(client)
    rsp = admin.create_user('test', 'test', ['read', 'write', 'masking-admin'])
    print(rsp)
    client.close()
    client.connect('test', 'test')
    return client


def maskless_client():
    client = Client("localhost", 3000)
    client.connect('admin', 'admin')
    admin = Admin(client)
    rsp = admin.create_user('zorro', 'test', ['read', 'write', 'read-masked', 'write-masked'])
    print(rsp)
    client.close()
    client.connect('zorro', 'test')
    return client


def setup_masking(client):
    client.info(
        f"masking:namespace=test;"
        f"set=demo;"
        f"bin=mystring;"
        f"type=string;"
        # f"function=remove;"
        f"function=constant;"
        f"value=FOOBARBAZ!"
    )


def main():
    print("Starting main")
    client = rw_client()
    print("Created rw client")
    noMask = maskless_client()
    print("Created maskless client")
    key = ("test", "demo", 0)
    # time.sleep(5)

    # setup_masking(client)
    # print("Created masking rule")
    # time.sleep(5)
    
    # Initialize string operations
    masked_ops = StringOperations(client)
    string_ops = StringOperations(noMask)
    
    bVal = "hello world"
    print(f"bVal: {bVal}")
    # Setup: Create a record with a string bin
    # noMask.delete(key)
    # noMask.put(key, [("mystring", bVal)], sendkey=False)
    # print("Initial record:", client.get(key))
    # print(noMask.get(key))
    
    # Test string operations (note: server-side may not be fully implemented yet)
    print("\n--- Testing String Operations ---")
    
    # Test strlen - get string length
    print("\n1. Testing strlen:")
    try:
        result = string_ops.strlen(key, "mystring")
        print(f"   strlen result: {result}")
        result = masked_ops.strlen(key, "mystring")
        print(f"   masked strlen result: {result}")
    except Exception as e:
        print(f"   strlen error (expected if not implemented): {e}")
    
    # Test substr - get substring
    offset, length = 1, 4
    print(f"\n2. Testing substr(offset={offset}, length={length}):")
    try:
        result = string_ops.substr(key, "mystring", offset=offset, length=length)
        print(f"   substr result: {result}")
    except Exception as e:
        print(f"   substr error (expected if not implemented): {e}")


    # Test find - find substring
    print(f"\n3. Testing find('world') in '{bVal}':")
    try:
        result = string_ops.find(key, "mystring", "world")
        print(f"   find result: {result}")
    except Exception as e:
        print(f"   find error (expected if not implemented): {e}")

    # Test find substring w/ occurrenceNum
    print(f"\n4. Testing find 2nd occurrence of 'l' in {bVal}:")
    try:
        result = string_ops.find(key, "mystring", "l", 2)
        print(f"   find result: {result}")
    except Exception as e:
        print(f"   find error (expected if not implemented): {e}")
    
    # Test find substring w/ not-found
    print(f"\n5. Testing find 'not-found' in {bVal}:")
    try:
        result = string_ops.find(key, "mystring", "not-found")
        print(f"   find result: {result}")
    except Exception as e:
        print(f"   find error (expected if not implemented): {e}")
    exit(0)
    
    # Test upper - convert to uppercase
    print("\n3. Testing upper:")
    try:
        result = string_ops.upper(key, "mystring")
        print(f"   upper result: {result}")
        print(f"   after upper: {client.get(key)}")
    except Exception as e:
        print(f"   upper error (expected if not implemented): {e}")
    
    # Reset string
    client.put(key, [("mystring", "hello world")], sendkey=False)
    
    # Test lower - convert to lowercase (on fresh data)
    print("\n4. Testing lower:")
    client.put(key, [("mystring", "HELLO WORLD")], sendkey=False)
    try:
        result = string_ops.lower(key, "mystring")
        print(f"   lower result: {result}")
        print(f"   after lower: {client.get(key)}")
    except Exception as e:
        print(f"   lower error (expected if not implemented): {e}")
    
    # Test insert - insert string at offset
    print("\n5. Testing insert:")
    client.put(key, [("mystring", "hello world")], sendkey=False)
    try:
        result = string_ops.insert(key, "mystring", offset=5, insert_str=" beautiful")
        print(f"   insert result: {result}")
        print(f"   after insert: {client.get(key)}")
    except Exception as e:
        print(f"   insert error (expected if not implemented): {e}")
    
    # Cleanup
    client.delete(key)
    client.close()
    print("\n--- Done ---")

if __name__ == "__main__":
    main()