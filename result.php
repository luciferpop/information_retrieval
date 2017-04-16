<?php
	include('query.php');
	// new query object
	$q = new query();
	$type = $_POST['type'];
	$query_str = $_POST['query'];
	$terms = $q->get_posting($query_str, $type);
?>
<!DOCTYPE html>
<html>

<head>
	<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
	<title>Query Results</title>
	<link rel="stylesheet" type="text/css" href="styles.css" />
</head>
	
<body>
	<div id="page2">
		<h2>Total <?php echo sizeof($terms);?> results for query "<?php echo $query_str;?>"</h2><br>
		<?php
			$conn = $q->get_conn();

			$page_size = 10;		// number of articles to display for every page
			$page = 1;				// current page
			$row_count = sizeof($terms);
			$total = 0;				// total number of pages

			if (!empty($_GET['page'])) {
				$page = $_GET['page'];
			}

			$total = ceil(($row_count / $page_size));
			$pre = ($page-1) * $page_size;

			// Preparing query
			$filename = [];
			foreach ($terms as $term) {
				array_push($filename, $term."newsML.xml");
			}
			$sql = "SELECT * FROM outline WHERE filename IN ('".implode("','",$filename)."') LIMIT ".$pre.", ".$page_size."";
			$res = mysqli_query($conn, $sql);
			while($row = mysqli_fetch_array($res)){
			    echo "<strong><font size='4' color='orange'><li>".$row['filename']."</li></font></strong>";
			    echo "<strong><font size='3'>".$row['title']."</font></strong><br>";
			    echo "<strong><font size='3'>".$row['headline']."</font></strong><br><br>";
			}
			if($page > 1){
			    $prePage = $page - 1;
			    echo "<a href='result.php?page=$prePage'>pre</a> ";
			}
			if($page < $total){
			    $nextPage = $page + 1;
			    echo "<a href='result.php?page=$nextPage'>next</a> ";
			    echo "{$page} / {$total} pages";
			  }
			echo "<br/><br/>";
		?>
	</div>
</body>

</html>
