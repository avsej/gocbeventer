// cbevent-version: 1

function on_mutation(item) {
    console.log('handling event...');

    setTimeout(function() {
        console.log('acking event...');

        http.get('http://localhost:3001/lolevent?key=' + item.key, function(err, resp) {
            console.log(err, resp);
        });

        item.ack();
    }, 500)
}
