<?php

?>

<!DOCTYPE html>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
<title>Reuters News Search Engine</title>

<link rel="stylesheet" type="text/css" href="styles.css" />

</head>

<body>

<div id="page">

	<h2>Reuters News Retrieval System</h2>

    <form id="searchForm" method="post" action="result.php" >
		<fieldset>
        
           	<input id='s' type="text" name="query" autocomplete="off" />
            
            <input type="submit" value="Submit" id="submitButton" />
            
            <div id="searchInContainer">
                <input type="radio" name="type" value="uniword"checked />
                <label for="uniwordSearch">Uniword</label>
                
                <input type="radio" name="type" value="biword"/>
                <label for="biwordSearch">Biword</label>
			</div>
            
        </fieldset>
    </form>

    <div id="resultsDiv"></div>
    
</div>

<p class="credit"><a href="">Lihua Zhang & Yameng Sun | Zine EOOD © 2009-2017</a></p>

</body>
</html>