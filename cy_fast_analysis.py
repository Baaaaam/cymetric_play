import cymetric as cym
import pandas as pd
import numpy as np



def get_transaction_TS(db, sender, receiver):
  evaler = cym.Evaluator(db)
  
  
# get transation & Agent tables
  trans = evaler.eval('Transactions')
  agents = evaler.eval('AgentEntry')
  # build 2 table for SenderId and ReceiverId
  agents_Receiver = agents.rename(index=str, columns={'AgentId': 'ReceiverId'})
  selected_receiver = agents_Receiver.loc[lambda df: df.Prototype == receiver,:]
  if selected_receiver.empty:
    print("unknown Receiver, available Receiver are:")
    for receiver_name in agents_Receiver.Prototype.unique():
      print(receiver_name)

    agents_Sender = agents.rename(index=str, columns={'AgentId': 'SenderId'})
  selected_sender = agents_Sender.loc[lambda df: df.Prototype == sender,:]
  if selected_sender.empty:
    print("unknown Sender, available Sender are:")
    for sender_name in agents_Sender.Prototype.unique():
      print(sender_name)

    if( selected_sender.empty and selected_receiver.empty):
    toplot = 0
  else: 
    selected_trans = trans.loc[trans['ReceiverId'].isin(selected_receiver.ReceiverId)]
        selected_trans = selected_trans.loc[selected_trans['SenderId'].isin(selected_sender.SenderId)]
    
    df = pd.merge(selected_sender[['SimId', 'SenderId', 'Prototype']], selected_trans, on=['SimId', 'SenderId'])
    df = df.rename(index=str, columns={'Prototype': 'SenderProto'})
    df = df.drop('SenderId',1)
    
    df = pd.merge(selected_receiver[['SimId', 'ReceiverId', 'Prototype']], df, on=['SimId', 'ReceiverId'])
    df = df.rename(index=str, columns={'Prototype': 'ReceiverProto'})
    df = df.drop('ReceiverId',1)

    resource = evaler.eval('Resources')
    selected_resources = resource.loc[resource['ResourceId'].isin(selected_trans.ResourceId)]

    df = pd.merge(selected_resources[['SimId', 'ResourceId','QualId','Quantity','Units'  ]], df, on=['SimId', 'ResourceId'])
    df = df.drop('ResourceId',1)

    grouped_trans = df[['ReceiverProto', 'SenderProto','Time', 'Quantity']].groupby(['ReceiverProto', 'SenderProto','Time']).sum()

    toplot = grouped_trans.loc[receiver].loc[sender]
  
  
  return toplot


