<?php
	class query {
		
		// Get database connection
		function get_conn() {
		    // $servername = "mysql1.cs.clemson.edu";
		    // $username = "infrtrvl_6ymy";
		    // $password = "z530628Z";
		    // $dbname = "info_retrieval_lwsh";
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
			$keywords = preg_split('/[\s,]+/', $str);

			switch($type) {
				case 'uniword':
					$temp = $this->fetch_res_uni($keywords, $type);
					// Just return an empty array when query result has empty 
					if (empty($temp)) {
						return $temp;
					}
					$posting = $this->to_array($temp);
					return $this->intersection($posting, 0, sizeof($posting)-1);
				case 'biword':
					return $this->fetch_res_bi($str, $type);
				case 'positional':
					return;
				case 'ranked':
					return $this->fetch_res_ra($keywords);
				case 'bm25':
					return $this->fetch_res_bm25($keywords);

			}
		}

		// Return top 20 results
		function get_top($result) {
			$res = [];
			arsort($result);
			$i = 0;
			foreach ($result as $key => $value) {
				if ($i < 20) {
					array_push($res, $key);
					$i++;
				} else {
					break;
				}
			}
			return $res;
		}

		// Return a hashmap which key is term and value is another hashmap of <fileName, tf-idf> pair
		function get_map($keywords, $table) {
			$map = array();
			$conn = $this->get_conn();
			$sql = "SELECT * FROM ".$table." WHERE term IN ('".implode("','",$keywords)."')";
			$res = mysqli_query($conn, $sql);
			while ($row = mysqli_fetch_row($res)) {
				$term = $row['0'];
				$temp = preg_split("/\t/", $row['1']);
				if (!array_key_exists($term, $map)) {
					$map[$term] = array();
				}
				$map[$term][$temp[0]] = $temp[1];
			}
			return $map;
		}

		// Fetch results for BM25
		function fetch_res_bm25($keywords) {
			$k1 = 1.2;
			$b = 0.75;
			$conn = $this->get_conn();
			$result = array();
			// Fetch the idf vector for the query
			$idf_qd = array();
			foreach ($keywords as $keyword) {
				$sql = "SELECT idf FROM idf WHERE term = '".$keyword."'";
				$res = mysqli_query($conn, $sql);
				$row = mysqli_fetch_row($res);
				$idf = preg_split("/[!\t]+/", $row[0], -1, PREG_SPLIT_NO_EMPTY);
				array_push($idf_qd, $idf[0]);
			}

			// Fetch the tf and document length vector, and save the Okapi BM25 score
			$mapArr = $this->get_map($keywords, "tf");
			foreach ($mapArr as $map => $subMap) {
				foreach ($subMap as $key => $value) {
					if (array_key_exists($key, $result))
						continue;
					$tf_qd = array();
					foreach ($mapArr as $term => $file) {
						$val = 0.0;
						if (array_key_exists($key, $file)) {
							$val = $file[$key];
						}
						array_push($tf_qd, $val);
					}
					//print_r($tf_qd);
					// Fetch the document length
					$sql_len = "SELECT length FROM length WHERE filename = '".$key."'";
					$res_len = mysqli_query($conn, $sql_len);
					$row_len = mysqli_fetch_row($res_len);
					$length = $len[$key];
					
					// Calculate Okapi BM25 score
					$score = 0;
					for ($i=0; $i<sizeof($idf_qd); $i++) {
						$left = $idf_qd[$i] * $tf_qd[$i] * ($k1+1);
						$right = $tf_qd[$i] + $k1 * (1.0 - $b + $b * $length / 3.0);
						if ($left == 0 || $right == 0) {
							continue;
						}
						$score += $left / $right;
					}

					$result[$key] = $score;
				} 
			}

			return $this->get_top($result);
		}

		// Fetch results for ranked query
		function fetch_res_ra($keywords) {
			$conn = $this->get_conn();
			$result = array();

			// Calculate tfidf vector for the query
			$tfidf = array();
			foreach ($keywords as $keyword) {
				if (array_key_exists($keyword, $tfidf)) {
					$tfidf[$keyword] = $tfidf[$keyword] + 1;
				} else {
					$tfidf[$keyword] = 1;
				}
			}

			foreach ($tfidf as $key => $feq) {
				// Calculate tf first
				$tfidf[$key] = $feq / sizeof($keywords);
				// Retrieve idf for query terms from database
				$sql = "SELECT idf FROM idf WHERE term='".$key."'";
				$res = mysqli_query($conn, $sql);
				$row = mysqli_fetch_row($res);
				$idf = preg_split("/[!\t]+/", $row[0], -1, PREG_SPLIT_NO_EMPTY);
				// Calculate tfidf for key
				$tfidf[$key] = $idf[0] * $tfidf[$key];
			}

			// Calculate the query vector length
			$query_len = 0.0;
			foreach ($tfidf as $key => $value) {
				$query_len += $value * $value;
			}
			$query_len = sqrt($query_len);
			
			$mapArr = $this->get_map($keywords, "tfidf");
			foreach ($mapArr as $map => $subMap) {
				foreach ($subMap as $key => $value) {
					if (array_key_exists($key, $result))
						continue;
					$tfidf_doc = array();
					foreach ($mapArr as $term => $file) {
						$val = 0.0;
						if (array_key_exists($key, $file)) {
							$val = $file[$key];
						}
						array_push($tfidf_doc, $val);
					}
					// Calculate doc vector length
					$doc_len = 0.0;
					foreach ($tfidf_doc as $idx => $value) {
					 	$doc_len += $value * $value;
					}
					$doc_len = sqrt($doc_len);

					// Calculate dot product for query and document
					$dot_product = 0.0;
					$i = 0;
					foreach ($tfidf as $k => $v) {
						$dot_product = $dot_product + $v * $tfidf_doc[$i];
						$i++;
					}

					// Calculate cosine similarity
					$sim = $dot_product / ($query_len * $doc_len);
					$result[$key] = $sim;
				} 
			}
			return $this->get_top($result);
		}

		// Fetch results for biword query
		function fetch_res_bi($keywords, $type) {
			$posting = [];
			$conn = $this->get_conn();

			$sql = 'SELECT * FROM '.$type.' WHERE term="'.$keywords.'"';
			$res = mysqli_query($conn, $sql);
			while ($row = mysqli_fetch_array($res)) {
				$posting = $row['posting'];
			}
			if (empty($posting)) {
				return;
			}
			$temp = preg_split("/\D+/", $posting, -1, PREG_SPLIT_NO_EMPTY);
			// Sort the array in ascending order by file name
			// sort($temp);
			return $temp;
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
				if ($left[$i] === $right[$j]) {
					array_push($res, $left[$i]);
					$i++;
					$j++;
				} else if ($left[$i] < $right[$j]) {
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