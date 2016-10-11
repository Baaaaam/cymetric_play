import cymetric as cym
import pandas as pd
import numpy as np



def get_transaction_TS(db, fac_a, fac_b):
  evaler = cym.Evaluator(db)
  
  
# get transation & Agent tables
  trans = evaler.eval('Transactions')
  agents = evaler.eval('AgentEntry')
  
# build 2 table for SenderId and ReceiverId
  agents_Sender = agents.rename(index=str, columns={'AgentId': 'SenderId'})
  agents_Receiver = agents.rename(index=str, columns={'AgentId': 'ReceiverId'})

#Merge SenderId into Transaction
  df = pd.merge(agents_Sender[['SimId', 'SenderId', 'Prototype']], trans, on=['SimId', 'SenderId'])
#rename Proto into SenderProto
  df = df.rename(index=str, columns={'Prototype': 'SenderProto'})
#remove SenderId from transation table (cleaning)
  df = df.drop('SenderId',1)
  print("merge1 ok")
#Same for Receiver ...
  df = pd.merge(agents_Receiver[['SimId', 'ReceiverId', 'Prototype']], df, on=['SimId', 'ReceiverId'])
  df = df.rename(index=str, columns={'Prototype': 'ReceiverProto'})
  df = df.drop('ReceiverId',1)
  print("merge 2 ok")


# Get resource Table
  resource = evaler.eval('Resources')
  df = pd.merge(resource[['SimId', 'ResourceId','QualId','Quantity','Units'  ]], df, on=['SimId', 'ResourceId'])
  df = df.drop('ResourceId',1)
  print("merge 3 ok")


  trans = df[['ReceiverProto', 'SenderProto','Time', 'Quantity']].groupby(['ReceiverProto', 'SenderProto','Time']).sum()
  print("group  ok")
  
  index = trans.index.levels[0]

  a_in = False
  b_in = False

  for word in index:
    if(fac_a == word)
      a_in = True
    if(fac_b == word)
      b_in = True
  
  if a_in && b_in):
    toplot = trans.loc[fac_b].loc[fac_a]
  else
    print(word)
    return
  print("selection ok")

return toplot


