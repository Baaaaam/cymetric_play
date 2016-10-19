import cymetric as cym
import pandas as pd
import numpy as np


def get_transaction_TS(db, sender, receiver):
  #initiate evaluation
  evaler = cym.Evaluator(db)
  
  # get transation & Agent tables
  trans = evaler.eval('Transactions')
  agents = evaler.eval('AgentEntry')
  now = timer()
  print('trans + agent done:', now-start)
  start = timer()
  
# build 2 table for SenderId and ReceiverId
  # get receiver
  agents_Receiver = agents.rename(index=str, columns={'AgentId': 'ReceiverId'})
  selected_receiver = agents_Receiver.loc[lambda df: df.Prototype == receiver,:]
  # check if receiver exists
  if selected_receiver.empty:
    print("unknown Receiver, available Receiver are:")
    for receiver_name in agents_Receiver.Prototype.unique():
      print(receiver_name)
  
  # get sender
  agents_Sender = agents.rename(index=str, columns={'AgentId': 'SenderId'})
  selected_sender = agents_Sender.loc[lambda df: df.Prototype == sender,:]
  # check if sender exists
  if selected_sender.empty:
    print("unknown Sender, available Sender are:")
    for sender_name in agents_Sender.Prototype.unique():
      print(sender_name)

  #check if seider and receiver exist
  if( selected_sender.empty or selected_receiver.empty):
    trans_table = 0
  else: 
  # Both receiver and sender exist:
    # select good corresponding transaction
    selected_trans = trans.loc[trans['ReceiverId'].isin(selected_receiver.ReceiverId)]
    selected_trans = selected_trans.loc[selected_trans['SenderId'].isin(selected_sender.SenderId)]
    
    # Merge Sender infos
    df = pd.merge(selected_sender[['SimId', 'SenderId', 'Prototype']], selected_trans, on=['SimId', 'SenderId'])
    df = df.rename(index=str, columns={'Prototype': 'SenderProto'})
    df = df.drop('SenderId',1)
    
    # Merge reveiver infos
    df = pd.merge(selected_receiver[['SimId', 'ReceiverId', 'Prototype']], df, on=['SimId', 'ReceiverId'])
    df = df.rename(index=str, columns={'Prototype': 'ReceiverProto'})
    df = df.drop('ReceiverId',1)

    # Get resource and select the proper one
    resource = evaler.eval('Resources')
    selected_resources = resource.loc[resource['ResourceId'].isin(selected_trans.ResourceId)]

    # merge Resource into transaction
    df = pd.merge(selected_resources[['SimId', 'ResourceId','QualId','Quantity','Units'  ]], df, on=['SimId', 'ResourceId'])
    df = df.drop('ResourceId',1)

    grouped_trans = df[['ReceiverProto', 'SenderProto','Time', 'Quantity']].groupby(['ReceiverProto', 'SenderProto','Time']).sum()

    trans_table = grouped_trans.loc[receiver].loc[sender]
  return trans_table

def get_inventory(db, facility):
  #initiate evaluation
  evaler = cym.Evaluator(db)

  # Get inventory table
  inv = evaler.eval('ExplicitInventory')

  selected_inv = inv.loc[lambda df: df.InventoryName == facility,:]