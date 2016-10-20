import cymetric as cym
import pandas as pd
import numpy as np
from timeit import default_timer as timer
import copy


def get_transaction_TS(db, sender, receiver):
  start = timer()
  #initiate evaluation
  evaler = cym.Evaluator(db)
  
  now = timer()
  print('evaluator done:', now-start)
  start = timer()
  
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
  now = timer()
  print('receiver select done:', now-start)
  start = timer()

  # get sender
  agents_Sender = agents.rename(index=str, columns={'AgentId': 'SenderId'})
  selected_sender = agents_Sender.loc[lambda df: df.Prototype == sender,:]
  # check if sender exists
  if selected_sender.empty:
    print("unknown Sender, available Sender are:")
    for sender_name in agents_Sender.Prototype.unique():
      print(sender_name)
  now = timer()
  print('sender select done:', now-start)
  start = timer()

  #check if seider and receiver exist
  if( selected_sender.empty or selected_receiver.empty):
    toplot = 0
  else: 
  # Both receiver and sender exist:
    # select good corresponding transaction
    selected_trans = trans.loc[trans['ReceiverId'].isin(selected_receiver.ReceiverId)]
    selected_trans = selected_trans.loc[selected_trans['SenderId'].isin(selected_sender.SenderId)]
    now = timer()
    print('trans select done:', now-start)
    start = timer()
    
    # Merge Sender infos
    df = pd.merge(selected_sender[['SimId', 'SenderId', 'Prototype']], selected_trans, on=['SimId', 'SenderId'])
    df = df.rename(index=str, columns={'Prototype': 'SenderProto'})
    df = df.drop('SenderId',1)
    now = timer()
    print('sender merged done:', now-start)
    start = timer()
    # Merge reveiver infos
    df = pd.merge(selected_receiver[['SimId', 'ReceiverId', 'Prototype']], df, on=['SimId', 'ReceiverId'])
    df = df.rename(index=str, columns={'Prototype': 'ReceiverProto'})
    df = df.drop('ReceiverId',1)
    now = timer()
    print('receiver merged done:', now-start)
    start = timer()

    # Get resource and select the proper one
    resource = evaler.eval('Resources')
    selected_resources = resource.loc[resource['ResourceId'].isin(selected_trans.ResourceId)]
    now = timer()
    print('resource selected done:', now-start)
    start = timer()

    # merge Resource into transaction
    df = pd.merge(selected_resources[['SimId', 'ResourceId','QualId','Quantity','Units'  ]], df, on=['SimId', 'ResourceId'])
    df = df.drop('ResourceId',1)
    now = timer()
    print('resource merged done:', now-start)
    start = timer()

    grouped_trans = df[['ReceiverProto', 'SenderProto','Time', 'Quantity']].groupby(['ReceiverProto', 'SenderProto','Time']).sum()

    toplot = grouped_trans.loc[receiver].loc[sender]
  
  
  return toplot


def get_inventory(db, facility):
  start = timer()
  #initiate evaluation
  evaler = cym.Evaluator(db)
  now = timer()
  print('evaler  done:', now-start)
  start = timer()
  
  # Get inventory table
  inv = evaler.eval('ExplicitInventory')
  agents = evaler.eval('AgentEntry')
  now = timer()
  print('table ok done:', now-start)
  start = timer()

  selected_agents = agents.loc[lambda df: df.Prototype == facility,:]
  if selected_agents.empty:
    print("unknown Facitlity, available Facilities are:")
    for fac_name in inv.Prototype.unique():
      print(fac_name)
    inv_table = 0
    now = timer()
    print('fac selected done:', now-start)
    start = timer()
  else:
    selected_inv = inv.loc[inv['AgentId'].isin(selected_agents.AgentId)]
    now = timer()
    print('selected done:', now-start)
    start = timer()
    
    df = pd.merge(selected_agents[['SimId', 'AgentId', 'Prototype']], selected_inv, on=['SimId', 'AgentId'])
    df = df.drop('AgentId',1)
    now = timer()
    print('merged done:', now-start)
    start = timer()
    
    inv_table = df[['Prototype', 'Time','Quantity']].groupby(['Prototype', 'Time']).sum()
    now = timer()
    print('grouped done:', now-start)
    start = timer()

  return inv_table
