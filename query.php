<?php
	class query {
		
		// Get database connection
		function get_conn() {
			$servername = "localhost";
		    $username = "root";
		    $password = "123";
		    $dbname = "ir_test";

		    // Create connection
		    $conn = mysqli_connect($servername, $username, $password, $dbname);
		    // Check connection
		    if (!$conn) {
		        die("Connection failed: " . mysqli_connect_error());
		    }

		    return $conn;
		}

		// Return posting from the SQL query
		function get_posting($str, $type) {
			$keyword = [];

			switch($type) {
				case 'uniword':
					$keywords = preg_split('/[\s,]+/', $str);
					$temp = $this->fetch_res_uni($keywords, $type);
					// Just return an empty array when query result has empty 
					if (empty($temp)) {
						return $temp;
					}
					$posting = $this->to_array($temp);
					// Sort the posting array according to the length of each posting
					usort($posting, function($a, $b) {
						return sizeof($a) - sizeof($b);
					});
					return $this->intersection($posting, 0, sizeof($posting)-1);
					break;
				case 'biword':
					array_push($keywords, $str);
					$posting = $this->fetch_res_bi($keywords, $type);
					break;
			}
		}

		// Fetch results for biword query
		function fetch_res_bi($keywords, $type) {
			$posting = [];
			$conn = $this->get_conn();

			$sql = 'SELECT * FROM '.$type.' WHERE term='.$keywords.'';
			$res = mysqli_query($conn, $sql);
			while ($row = mysqli_fetch_array($res)) {
				array_push($posting, $row['posting'])
			}

			return this->to_array($posting);
			$conn.close();
		}

		// Fetch results for uniword query
		function fetch_res_uni($keywords, $type) {
			$posting = [];
			$conn = $this->get_conn();

			$sql = [];
			$s = 'SELECT * FROM '.$type.' WHERE term =';
			foreach($keywords as $keyword) {
				array_push($sql, $s.'"'.strtolower($keyword).'"');
			}

			foreach($sql as $curr) {
				$temp = mysqli_query($conn, $curr);
				if ($temp->num_rows == 0) {
					$empty = [];
					return $empty;
				}
				while ($row = mysqli_fetch_array($temp)) {
					array_push($posting, $row['posting']);
				}
			}
			return $posting;
			$conn.close();
		}

		// Using Divide and Conqure do intersection for the given postings (uniword query only)
		function intersection($posting, $start, $end) {
			if ($start >= $end) {
				return $posting[$start];
			}

			$mid = ($end - $start)/2 + $start;
			$left = $this->intersection($posting, $start, $mid);
			$right = $this->intersection($posting, $mid+1, $end);

			return $this->helper($left, $right);
		}

		// Intersection for two postings
		function helper($left, $right) {
			$res = [];
			$i = 0; $j = 0;
			while ($i < sizeof($left) && $j < sizeof($right)) {
				if (strcmp($left[$i], $right[$j]) == 0) {
					array_push($res, $left[$i]);
					$i++;
					$j++;
				} else if (strcmp($left[$i], $right[$j]) < 0) {
					$i++;
				} else {
					$j++;
				}
			}
			return $res;
		}

		// Convert posting array to array of file name array
		// i.e. {[f1, f2], .., [f3, f4, ..], ...} to {{f1, f2, ..}, {f3, f4, ..}, ...}
		function to_array($posting) {
			$res = [];

			foreach ($posting as $pos) {
				// Split by non-digit char for postings, only keep no empty string
				$temp = preg_split("/\D+/", $pos, -1, PREG_SPLIT_NO_EMPTY);
				// Sort the array in ascending order by file name
				sort($temp);
				array_push($res, $temp);
			}

			return $res;
		} 
	}
?>