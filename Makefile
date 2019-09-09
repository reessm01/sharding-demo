.PHONY: clean
.SILENT: clean

clean:
	rm -rf data/
	rm -f mapping.json
	echo "All done!"