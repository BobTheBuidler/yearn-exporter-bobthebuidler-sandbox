
from brownie import chain

vault = Contract('0x84e13785b5a27879921d6f685f041421c7f482da ')
topics = ['0xddf252ad00000000000000000000000000000000000000000000000000000000']
events = decode_logs(get_logs_asap(str(vat), topics, from_block= 0, to_block=chain[-1]))

print(events)
filtered = []
for event in event:
    print(event)