def get_param_defs(constants):
  return (
      ("Security", constants.security_tuple, True),
      ("Price", 0.0, False, 0, 10000000, 7),
      ("ValidSeconds", 300, True, 60),
      ("MinSize", 0, False, 0, 10000000),
      ("MaxPov", 0.0, False, 0, 1, 2),
      ("Aggression", ("Low", "Medium", "High", "Highest"), True),
  )
