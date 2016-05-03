import sys
sys.path.append("../../../")

from jpyutils.distributed import hadoop

left_input_path = "/app/ecom/fcr-opt/kr/analytics/2015-11-21/final/query_*"
left_fields_num = 19
left_key_list = "0,1,3"
left_value_list = "16,15"

right_input_path = "/app/ecom/fcr-opt/kr/analytics/2015-11-21/final/impress_*"
right_fields_num = 11
right_key_list = "0,1,3"
right_value_list = "9,4"

output_path = "/app/ecom/fcr-opt/kr/zhangjian09/testing/tools/hadoop/join"

h = hadoop.Hadoop()
join_cmd = h.join(left_input_path = left_input_path,
                  left_fields_num = left_fields_num,
                  left_key_list = left_key_list,
                  right_input_path = right_input_path,
                  right_fields_num = right_fields_num,
                  right_key_list = right_key_list,
                  output_path = output_path,
                  left_value_list = left_value_list,
                  right_value_list = right_value_list)

print join_cmd
h.run(join_cmd, clear_output = True)
