<?php
	include('query.php');
	error_reporting(0);
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
	<h1><?php echo sizeof($_SESSION['terms']);?> news found for query "<?php echo $_SESSION['query_str'];?>"</h1>

	<h3>Showing <?php $res = 1; if (isset($_GET['page'])) {$res = $_GET['page'];} echo ($res-1)*10+1;?>-<?php echo $res*10;?> of <?php echo sizeof($_SESSION['terms']);?> results</h3>

		<div id="resultsDiv">

			<div class="webResult">

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
					    echo "<h2><a>".$row['title']."</a></h2>";
					    echo "<p>".$row['paragraph']."</p>";
					    echo "<a href='detail.php?id=".$row['filename']."' target='_blank'>".$row['filename']."</a>";
					}
					echo "<div class='foot'>";
					if($page > 1){
					    $prePage = $page - 1;
					    echo "<a href='result.php?page=$prePage'>pre</a> ";
					}
					if($page < $total){
					    $nextPage = $page + 1;
					    echo "<a href='result.php?page=$nextPage'>next</a> ";
					    echo "{$page} / {$total} pages";
					  }

					echo "</div>";
				?>

			</div>
		</div>

	<p class="credit"><a href="index.php">Lihua Zhang & Yameng Sun | Zine EOOD © 2009-2017</a></p>

</body>

</html>
