<?php
	include('query.php');
	error_reporting(0);
	$q = new query();
?>
<!DOCTYPE html>
<html>
<head>
	<title>News Detail</title>
	<link rel="stylesheet" type="text/css" href="styles.css" />
</head>
<body>
	<?php
		$filename = $_GET['id'];
		$conn = $q->get_conn();

		$sql1 = "SELECT title FROM outline WHERE filename = '".$filename."'";
		$res1 = mysqli_query($conn, $sql1);
		$row1 = mysqli_fetch_row($res1);
		$title = $row1['0'];

		$sql2 = "SELECT paragraph from detail WHERE filename = '".preg_split('/\D+/', $filename)[0]."'";
		$res2 = mysqli_query($conn, $sql2);
		$row2 = mysqli_fetch_row($res2);
		$paragraph = $row2['0'];
	?>
	<h2 class="detail"><?php echo $title;?></h2>
	<p class="detail"><?php echo $paragraph;?></p>
</body>
</html>