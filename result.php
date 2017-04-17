<?php
	include('query.php');
	if (!isset($_SESSION)) {
		session_start();
	}

	// new query object
	$q = new query();
	$type = $_POST['type'];

	if (empty($_SESSION['terms'])) {
		$_SESSION['query_str'] = $_POST['query'];
		$_SESSION['terms'] = $q->get_posting($_SESSION['query_str'], $type);
	}

?>
<!DOCTYPE html>
<html>

<head>
	<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
	<title>Query Results</title>
	<link rel="stylesheet" type="text/css" href="styles.css" />
</head>
	
<body>
	<h1>Total <?php echo sizeof($_SESSION['terms']);?> results for query "<?php echo $_SESSION['query_str'];?>"</h1><br>


		<?php
			$conn = $q->get_conn();

			$page_size = 10;		// number of articles to display for every page
			$page = 1;				// current page
			$row_count = sizeof($_SESSION['terms']);
			$total = 0;				// total number of pages

			if (!empty($_GET['page'])) {
				$page = $_GET['page'];
			}

			$total = ceil(($row_count / $page_size));
			$pre = ($page-1) * $page_size;

			// Preparing query
			$filename = [];
			foreach ($_SESSION['terms'] as $term) {
				array_push($filename, $term."newsML.xml");
			}
			$sql = "SELECT * FROM outline WHERE filename IN ('".implode("','",$filename)."') LIMIT ".$pre.", ".$page_size."";
			$res = mysqli_query($conn, $sql);
			while($row = mysqli_fetch_array($res)){
			    echo "<li>".$row['filename']."</li>";
			    echo "<h3>".$row['title']."</h3><br>";
			    echo "<h3>".$row['headline']."</h3><br>";
			}
			echo "<p>";
			if($page > 1){
			    $prePage = $page - 1;
			    echo "<a href='result.php?page=$prePage'>pre</a> ";
			}
			if($page < $total){
			    $nextPage = $page + 1;
			    echo "<a href='result.php?page=$nextPage'>next</a> ";
			    echo "{$page} / {$total} pages";
			  }

			echo "</p>";
		?>

	<p class="credit"><a href="index.php">Lihua Zhang & Yameng Sun | Zine EOOD © 2009-2017</a></p>

</body>

</html>
