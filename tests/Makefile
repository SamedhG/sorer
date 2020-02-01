all:
	-./sorer -f 0.sor -print_col_type 0  
	-./sorer -f 0.sor -is_missing_idx 0 0
	-./sorer -f 1.sor -print_col_type 0  
	-./sorer -f 1.sor -is_missing_idx 0 1
	-./sorer -f 1.sor -print_col_idx 0 3
	-./sorer -f 2.sor -print_col_type 0
	-./sorer -f 2.sor -print_col_type 1
	-./sorer -f 2.sor -print_col_type 2
	-./sorer -f 2.sor -print_col_type 3
	-./sorer -f 2.sor -is_missing_idx 1 0
	-./sorer -f 2.sor -is_missing_idx 1 1
	-./sorer -f 2.sor -print_col_idx 3 0
	-./sorer -f 2.sor -print_col_idx 3 1
	-./sorer -f 2.sor -print_col_idx 2 0
	-./sorer -f 2.sor -print_col_idx 2 1
	-./sorer -f 3.sor -print_col_type 4
	-./sorer -f 3.sor -print_col_idx 2 10
	-./sorer -f 3.sor -print_col_idx 2 384200
	-./sorer -f 4.sor -print_col_idx 0 1
	-./sorer -f 4.sor -print_col_idx 0 2
	-./sorer -f 4.sor -print_col_idx 1 1
	-./sorer -f 4.sor -print_col_idx 1 2
	-./sorer -f 1.sor -from 1 -len 74 -print_col_type 0
	-./sorer -f 1.sor -from 1 -len 74 -is_missing_idx 0 0
	-./sorer -f 1.sor -from 1 -len 74 -print_col_idx 0 6

clean:
	- rm a.out *~
