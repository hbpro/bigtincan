package com.bigtincan.utils

object SystemParams {
  def getInputPath = {
    sys.env.get("BIGTINCAN_INPUT") orElse Option("/home/hossein/Desktop/Assignment")
  }
  def getOutputPath = {
    sys.env.get("BIGTINCAN_OUTPUT") orElse Option("/home/hossein/Desktop/Assignment/output")
  }
}
